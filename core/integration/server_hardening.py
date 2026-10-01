"""
Server-side hardening helpers.

Per-IP rate limiting, API-key/JWT authentication and structured logging of
every refused request. Framework-agnostic callables usable from any HTTP
handler.
"""

# [FEATURE: INTEGRATION_HARDENING] Durcissement serveur cote integrateur.
# Raison: limiter par IP, authentifier les appels et tracer chaque refus.
# Attention: tous les refus produisent une ligne de log JSON structuree.

import base64
import hashlib
import hmac
import json
import time
from typing import Callable, Dict, Optional

from core.rate_limiter.limiter import TokenBucketLimiter


class RateLimitMiddleware:
    """Strict per-IP token buckets; excess requests are refused, never queued."""

    def __init__(
        self,
        requests_per_sec: float,
        burst_capacity: float,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.requests_per_sec = requests_per_sec
        self.burst_capacity = burst_capacity
        self.clock = clock
        self._buckets: Dict[str, TokenBucketLimiter] = {}

    def _get_bucket(self, client_ip: str) -> TokenBucketLimiter:
        bucket = self._buckets.get(client_ip)
        if bucket is None:
            bucket = TokenBucketLimiter(rate=self.requests_per_sec, capacity=self.burst_capacity)
            self._buckets[client_ip] = bucket
        return bucket

    def is_allowed(self, client_ip: str) -> bool:
        bucket = self._get_bucket(client_ip)
        bucket._refill(int(self.clock() * 1_000_000_000))
        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            return True
        return False


class ApiKeyAuthenticator:
    """Constant-time API key comparison against a caller-provided SHA-256 hash."""

    def __init__(self, api_key_sha256: str):
        self.api_key_sha256 = api_key_sha256.lower()

    def is_valid(self, provided_key: str) -> bool:
        provided_hash = hashlib.sha256(provided_key.encode("utf-8")).hexdigest()
        return hmac.compare_digest(provided_hash, self.api_key_sha256)


class JwtAuthenticator:
    """Minimal HS256 JWT verifier: signature then expiry, no algorithm confusion."""

    def __init__(self, shared_secret: str):
        self.shared_secret = shared_secret.encode("utf-8")

    def is_valid(self, token: str) -> bool:
        try:
            header_b64, payload_b64, signature_b64 = token.split(".")
            header = json.loads(base64.urlsafe_b64decode(header_b64 + "=="))
            if header.get("alg") != "HS256":
                return False
            expected_signature = hmac.new(
                self.shared_secret,
                f"{header_b64}.{payload_b64}".encode("ascii"),
                hashlib.sha256,
            ).digest()
            if not hmac.compare_digest(expected_signature, base64.urlsafe_b64decode(signature_b64 + "==")):
                return False
            payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=="))
            expiry_sec = payload.get("exp")
            if expiry_sec is None or time.time() >= float(expiry_sec):
                return False
            return True
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            return False


class RejectedRequestLogger:
    """Structured JSON-lines logger for every refused request."""

    def __init__(self, sink: Callable[[str], None]):
        self.sink = sink

    def log_rejection(
        self,
        client_ip: str,
        reason: str,
        path: str,
        extra: Optional[Dict[str, object]] = None,
    ) -> None:
        log_entry = {
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "event": "request_rejected",
            "client_ip": client_ip,
            "reason": reason,
            "path": path,
        }
        if extra:
            log_entry.update(extra)
        self.sink(json.dumps(log_entry, sort_keys=True))
