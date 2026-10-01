"""
Unit tests for core/integration (integrator-side hardening toolkit).

All HTTP interactions use httpx.MockTransport or plain fakes: no real network.
"""

import base64
import hashlib
import hmac
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import httpx

from core.integration import (
    ApiKeyAuthenticator,
    JwtAuthenticator,
    OAuth2ClientCredentials,
    OfficialApiClient,
    PoliteClient,
    RateLimitMiddleware,
    RejectedRequestLogger,
    ServiceAllowlist,
    WafLogAnalyzer,
    WafStagingConfig,
    WafStagingValidator,
)


def _json_response(payload: dict, status_code: int = 200, headers: dict = None) -> httpx.Response:
    return httpx.Response(status_code=status_code, json=payload, headers=headers or {})


class TestServiceAllowlist(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "allowlist.json"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write_config(self, payload):
        self.config_path.write_text(json.dumps(payload), encoding="utf-8")

    def test_valid_config_allows_matching_ip_and_identity(self):
        self._write_config(
            {"allowed_ips": ["203.0.113.10"], "allowed_identities": ["billing-bot"]}
        )
        allowlist = ServiceAllowlist.from_json_file(self.config_path)
        self.assertTrue(allowlist.is_ip_allowed("203.0.113.10"))
        self.assertTrue(allowlist.is_identity_allowed("billing-bot"))
        self.assertTrue(allowlist.is_request_allowed("203.0.113.10", "billing-bot"))

    def test_missing_file_fails_closed(self):
        allowlist = ServiceAllowlist.from_json_file(self.config_path)
        self.assertFalse(allowlist.is_ip_allowed("203.0.113.10"))
        self.assertFalse(allowlist.is_identity_allowed("billing-bot"))

    def test_malformed_config_fails_closed(self):
        self.config_path.write_text("not-json", encoding="utf-8")
        allowlist = ServiceAllowlist.from_json_file(self.config_path)
        self.assertFalse(allowlist.is_ip_allowed("203.0.113.10"))

    def test_invalid_ip_string_is_rejected(self):
        self._write_config({"allowed_ips": [], "allowed_identities": ["bot"]})
        allowlist = ServiceAllowlist.from_json_file(self.config_path)
        self.assertFalse(allowlist.is_ip_allowed("not-an-ip"))


class TestOfficialApiClient(unittest.TestCase):
    def _build_api_key_client(self, transport):
        return OfficialApiClient(
            base_url="https://api.example.com",
            auth_mode="api_key",
            api_key="secret-key",
            transport=transport,
        )

    def test_api_key_header_is_sent(self):
        captured_headers = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured_headers.update(dict(request.headers))
            return _json_response({"ok": True})

        client = self._build_api_key_client(httpx.MockTransport(handler))
        response = client.get("/v1/status")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured_headers.get("x-api-key"), "secret-key")
        client.close()

    def test_invalid_auth_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            OfficialApiClient(base_url="https://api.example.com", auth_mode="cookie")

    def test_missing_api_key_is_rejected(self):
        with self.assertRaises(ValueError):
            OfficialApiClient(base_url="https://api.example.com", auth_mode="api_key")

    def test_oauth2_token_is_fetched_cached_and_attached(self):
        token_requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/oauth/token":
                token_requests.append(request)
                return _json_response({"access_token": "token-1", "expires_in": 3600})
            return _json_response({"ok": True})

        oauth2 = OAuth2ClientCredentials(
            token_url="https://api.example.com/oauth/token",
            client_id="client",
            client_secret="secret",
        )
        client = OfficialApiClient(
            base_url="https://api.example.com",
            auth_mode="oauth2",
            oauth2=oauth2,
            transport=httpx.MockTransport(handler),
        )
        client.get("/v1/data")
        client.get("/v1/data")
        self.assertEqual(len(token_requests), 1)
        client.close()

    def test_oauth2_invalid_expiry_is_rejected(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json_response({"access_token": "token-1", "expires_in": 0})

        oauth2 = OAuth2ClientCredentials(
            token_url="https://api.example.com/oauth/token",
            client_id="client",
            client_secret="secret",
            transport=httpx.MockTransport(handler),
        )
        with self.assertRaises(ValueError):
            oauth2.fetch_token()

    def test_mtls_requires_cert_and_key(self):
        with self.assertRaises(ValueError):
            OfficialApiClient(
                base_url="https://api.example.com",
                auth_mode="mtls",
                client_cert="cert.pem",
            )


class TestPoliteClient(unittest.TestCase):
    def _build_client(self, transport, requests_per_sec=5.0):
        return PoliteClient(
            base_url="https://api.example.com",
            user_agent="IntegrationBot/1.0 (contact@example.com)",
            requests_per_sec=requests_per_sec,
            transport=transport,
        )

    def test_user_agent_is_sent(self):
        captured_headers = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured_headers.update(dict(request.headers))
            return _json_response({"ok": True})

        client = self._build_client(httpx.MockTransport(handler))
        client.get("/health")
        self.assertIn("IntegrationBot", captured_headers.get("user-agent", ""))
        client.close()

    def test_empty_user_agent_is_rejected(self):
        with self.assertRaises(ValueError):
            PoliteClient(
                base_url="https://api.example.com",
                user_agent="",
                requests_per_sec=5.0,
            )

    def test_budget_exhaustion_fails_closed(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json_response({"ok": True})

        client = self._build_client(httpx.MockTransport(handler))
        client.limiter.tokens = 0.0
        with self.assertRaises(RuntimeError):
            client.get("/health")
        client.close()


class TestWafStaging(unittest.TestCase):
    def _build_config(self, paths=None):
        return WafStagingConfig(
            environment="staging",
            validation_paths=paths or ["/", "/health"],
        )

    def test_config_only_accepts_detection_mode(self):
        with self.assertRaises(ValueError):
            WafStagingConfig(environment="staging", mode="blocking")

    def test_checklist_contains_detection_checks(self):
        checklist = self._build_config().to_checklist()
        self.assertEqual(checklist["waf_mode"], "detection")
        self.assertTrue(all(isinstance(check, str) for check in checklist["checks"]))

    def test_validator_flags_blocked_legitimate_request(self):
        responses = {
            "/": httpx.Response(status_code=200, json={"ok": True}),
            "/health": httpx.Response(status_code=403, json={"blocked": True}),
        }
        validator = WafStagingValidator(self._build_config(), lambda path: responses[path])
        result = validator.validate_all()
        self.assertFalse(result["all_passed"])
        verdicts = {entry["path"]: entry["verdict"] for entry in result["results"]}
        self.assertEqual(verdicts["/"], "pass")
        self.assertEqual(verdicts["/health"], "false_positive")


class TestWafLogAnalyzer(unittest.TestCase):
    def test_false_positives_are_detected_and_ranked(self):
        log_lines = "\n".join(
            [
                json.dumps({"status": 403, "rule_id": "R1", "user_agent": "IntegrationBot/1.0"}),
                json.dumps({"status": 403, "rule_id": "R1", "user_agent": "IntegrationBot/1.0"}),
                json.dumps({"status": 403, "rule_id": "R2", "user_agent": "evil-scanner"}),
                json.dumps({"status": 200, "rule_id": "R9", "user_agent": "IntegrationBot/1.0"}),
                "not-json",
            ]
        )
        report = WafLogAnalyzer().analyze(log_lines)
        self.assertEqual(report.total_events, 5)
        self.assertEqual(report.blocked_events, 3)
        self.assertEqual(len(report.false_positives), 2)
        self.assertEqual(report.top_offending_rules, {"R1": 2})
        self.assertEqual(report.malformed_lines, 1)
        self.assertIn("R1", report.to_report()["recommended_actions"][0])

    def test_clean_log_requires_no_correction(self):
        report = WafLogAnalyzer().analyze(
            json.dumps({"status": 200, "rule_id": "R9", "user_agent": "IntegrationBot/1.0"})
        )
        self.assertEqual(report.false_positives, [])
        self.assertIn("No correction", report.to_report()["recommended_actions"][0])


class TestServerHardening(unittest.TestCase):
    def test_rate_limit_allows_burst_then_refuses(self):
        middleware = RateLimitMiddleware(requests_per_sec=2.0, burst_capacity=3.0)
        outcomes = [middleware.is_allowed("10.0.0.1") for _ in range(5)]
        self.assertEqual(outcomes, [True, True, True, False, False])

    def test_rate_limit_is_per_ip(self):
        middleware = RateLimitMiddleware(requests_per_sec=1.0, burst_capacity=1.0)
        self.assertTrue(middleware.is_allowed("10.0.0.1"))
        self.assertFalse(middleware.is_allowed("10.0.0.1"))
        self.assertTrue(middleware.is_allowed("10.0.0.2"))

    def test_api_key_authenticator_validates_hash(self):
        key_hash = hashlib.sha256(b"secret-key").hexdigest()
        authenticator = ApiKeyAuthenticator(api_key_sha256=key_hash)
        self.assertTrue(authenticator.is_valid("secret-key"))
        self.assertFalse(authenticator.is_valid("wrong-key"))

    def test_jwt_authenticator_accepts_valid_hs256_token(self):
        shared_secret = "unit-test-secret"
        header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256"}).encode()).decode().rstrip("=")
        payload = base64.urlsafe_b64encode(json.dumps({"exp": 4102444800}).encode()).decode().rstrip("=")
        signing_input = f"{header}.{payload}".encode("ascii")
        signature = base64.urlsafe_b64encode(
            hmac.new(shared_secret.encode(), signing_input, hashlib.sha256).digest()
        ).decode().rstrip("=")
        authenticator = JwtAuthenticator(shared_secret=shared_secret)
        self.assertTrue(authenticator.is_valid(f"{header}.{payload}.{signature}"))

    def test_jwt_authenticator_rejects_wrong_signature_and_alg(self):
        shared_secret = "unit-test-secret"
        header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256"}).encode()).decode().rstrip("=")
        payload = base64.urlsafe_b64encode(json.dumps({"exp": 4102444800}).encode()).decode().rstrip("=")
        bad_signature = base64.urlsafe_b64encode(b"tampered").decode().rstrip("=")
        authenticator = JwtAuthenticator(shared_secret=shared_secret)
        self.assertFalse(authenticator.is_valid(f"{header}.{payload}.{bad_signature}"))
        none_header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode()).decode().rstrip("=")
        self.assertFalse(authenticator.is_valid(f"{none_header}.{payload}.{bad_signature}"))

    def test_rejected_requests_are_logged_structured(self):
        logged_lines = []
        logger = RejectedRequestLogger(sink=logged_lines.append)
        logger.log_rejection("10.0.0.9", "rate_limited", "/checkout", {"rule": "ip_burst"})
        self.assertEqual(len(logged_lines), 1)
        entry = json.loads(logged_lines[0])
        self.assertEqual(entry["event"], "request_rejected")
        self.assertEqual(entry["client_ip"], "10.0.0.9")
        self.assertEqual(entry["reason"], "rate_limited")
        self.assertEqual(entry["rule"], "ip_burst")


if __name__ == "__main__":
    unittest.main()
