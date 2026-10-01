"""
Polite HTTP client: explicit User-Agent plus strict token-bucket pacing.
"""

# [FEATURE: INTEGRATION_HARDENING] Client HTTP poliment cadence.
# Raison: identification claire du client et respect strict du debit autorise.
# Attention: reutilise TokenBucketLimiter, aucune requete ne passe sans token.

from typing import Any, Optional

import httpx

from core.rate_limiter.limiter import TokenBucketLimiter


class PoliteClient:
    """Synchronous client that refuses to send before the token bucket allows it."""

    def __init__(
        self,
        base_url: str,
        user_agent: str,
        requests_per_sec: float,
        burst_capacity: Optional[float] = None,
        timeout_sec: float = 10.0,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        if not user_agent:
            raise ValueError("user_agent must be explicit and non-empty")
        if requests_per_sec <= 0:
            raise ValueError("requests_per_sec must be strictly positive")
        self.user_agent = user_agent
        self.limiter = TokenBucketLimiter(
            rate=requests_per_sec,
            capacity=burst_capacity if burst_capacity is not None else max(1.0, requests_per_sec),
        )
        self.client = httpx.Client(
            base_url=base_url,
            timeout=timeout_sec,
            headers={"User-Agent": user_agent},
            transport=transport,
        )

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        if not self.limiter.try_acquire(1.0):
            raise RuntimeError("rate limit budget exhausted; request refused (fail-closed)")
        return self.client.request(method, path, **kwargs)

    def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", path, **kwargs)

    def close(self) -> None:
        self.client.close()
