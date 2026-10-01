"""
Authenticated client for an official API.

Supports API-key auth, OAuth2 client_credentials and mTLS. Credentials are
injected by the caller; nothing is hardcoded and no endpoint is bypassed.
"""

# [FEATURE: INTEGRATION_HARDENING] Client API officielle authentifie.
# Raison: l'acces officiel passe par cle API, OAuth2 client_credentials ou mTLS.
# Attention: le token OAuth2 est mis en cache et rafraichi seulement apres expiration.

import time
from typing import Any, Dict, Optional

import httpx


class OAuth2ClientCredentials:
    """Fetches and caches OAuth2 client_credentials tokens for one tenant."""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.transport = transport
        self._cached_token: Optional[str] = None
        self._token_expiry_monotonic: float = 0.0

    def fetch_token(self) -> str:
        with httpx.Client(transport=self.transport) as client:
            response = client.post(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
        response.raise_for_status()
        token_payload = response.json()
        expires_in_sec = int(token_payload.get("expires_in", 0))
        if expires_in_sec <= 0:
            raise ValueError("OAuth2 token response missing positive expires_in")
        self._cached_token = token_payload["access_token"]
        self._token_expiry_monotonic = time.monotonic() + expires_in_sec
        return self._cached_token

    def get_token(self) -> str:
        if self._cached_token is None or time.monotonic() >= self._token_expiry_monotonic:
            return self.fetch_token()
        return self._cached_token


class OfficialApiClient:
    """Synchronous client for one official API with configurable authentication."""

    def __init__(
        self,
        base_url: str,
        auth_mode: str,
        api_key: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        oauth2: Optional[OAuth2ClientCredentials] = None,
        client_cert: Optional[str] = None,
        client_key: Optional[str] = None,
        timeout_sec: float = 10.0,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        if auth_mode not in ("api_key", "oauth2", "mtls"):
            raise ValueError(f"unsupported auth_mode: {auth_mode}")
        if auth_mode == "api_key" and not api_key:
            raise ValueError("api_key required for auth_mode='api_key'")
        if auth_mode == "oauth2" and oauth2 is None:
            raise ValueError("oauth2 credentials required for auth_mode='oauth2'")
        if auth_mode == "mtls" and not (client_cert and client_key):
            raise ValueError("client_cert and client_key required for auth_mode='mtls'")

        self.auth_mode = auth_mode
        self.api_key = api_key
        self.api_key_header = api_key_header
        self.oauth2 = oauth2
        if self.oauth2 is not None and getattr(self.oauth2, "transport", None) is None:
            self.oauth2.transport = transport
        self.client = httpx.Client(
            base_url=base_url,
            timeout=timeout_sec,
            cert=(client_cert, client_key) if auth_mode == "mtls" else None,
            transport=transport,
        )

    def _build_headers(self) -> Dict[str, str]:
        if self.auth_mode == "api_key":
            return {self.api_key_header: str(self.api_key)}
        if self.auth_mode == "oauth2":
            return {"Authorization": f"Bearer {self.oauth2.get_token()}"}
        return {}

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        return self.client.request(method, path, headers=self._build_headers(), **kwargs)

    def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", path, **kwargs)

    def close(self) -> None:
        self.client.close()
