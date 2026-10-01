"""
High-Performance Rate Limiter
------------------------------
Implements:
1. TokenBucketLimiter: Microsecond token bucket supporting burst capacity and steady rate.
2. AdaptiveRateLimiter: Dynamic backoff reacting to HTTP 429 and Retry-After headers.
"""

import asyncio
import email.utils
import random
import time
from typing import Any, Dict, Optional

HTTP_TOO_MANY_REQUESTS = 429
HTTP_SERVICE_UNAVAILABLE = 503
THROTTLING_STATUS_CODES = (HTTP_TOO_MANY_REQUESTS, HTTP_SERVICE_UNAVAILABLE)
DEFAULT_PENALTY_MIN_SEC = 1.0
DEFAULT_PENALTY_MAX_SEC = 3.0
MAX_RETRY_AFTER_SEC = 300.0
RATE_CUT_FACTOR = 0.5
RATE_RECOVERY_FACTOR = 1.1
SUCCESSES_BEFORE_RECOVERY = 20


def parse_retry_after_seconds(retry_after_value: Optional[str]) -> Optional[float]:
    """
    Parses a Retry-After header: delta-seconds ("120") or HTTP-date (RFC 9110).
    Returns None if absent or unparseable. Capped at MAX_RETRY_AFTER_SEC.
    """
    if not retry_after_value:
        return None
    try:
        delay_sec = float(retry_after_value)
    except ValueError:
        try:
            retry_at = email.utils.parsedate_to_datetime(retry_after_value)
        except (TypeError, ValueError):
            return None
        if retry_at is None:
            return None
        delay_sec = retry_at.timestamp() - time.time()
    return min(MAX_RETRY_AFTER_SEC, max(0.0, delay_sec))


class TokenBucketLimiter:
    """
    Token Bucket Rate Limiter.
    Allows bursts up to `capacity` while strictly guaranteeing an average `rate` per second.
    """

    def __init__(self, rate: float, capacity: float):
        """
        :param rate: Number of tokens added per second.
        :param capacity: Maximum number of tokens that can accumulate (burst capacity).
        """
        self.rate = float(rate)
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.last_update_ns = time.perf_counter_ns()
        self._lock = asyncio.Lock()

    def _refill(self, now_ns: Optional[int] = None):
        if now_ns is None:
            now_ns = time.perf_counter_ns()
        if now_ns > self.last_update_ns:
            elapsed_sec = (now_ns - self.last_update_ns) / 1_000_000_000.0
            self.tokens = min(self.capacity, self.tokens + elapsed_sec * self.rate)
            self.last_update_ns = now_ns

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Non-blocking token check. Returns True if token was acquired immediately."""
        now_ns = time.perf_counter_ns()
        if now_ns < self.last_update_ns:
            return False
        self._refill(now_ns)
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0, min_start_ns: int = 0) -> float:
        """
        Asynchronous acquisition.
        Calculates required wait time inside the lock (respecting min_start_ns for penalties),
        then sleeps outside the lock to prevent serializing other concurrent tasks.
        Returns the duration waited in milliseconds.
        """
        async with self._lock:
            now_ns = time.perf_counter_ns()
            effective_now_ns = max(now_ns, min_start_ns)

            # Refill tokens up to effective start time
            if effective_now_ns > self.last_update_ns:
                elapsed_sec = (effective_now_ns - self.last_update_ns) / 1_000_000_000.0
                self.tokens = min(self.capacity, self.tokens + elapsed_sec * self.rate)
                self.last_update_ns = effective_now_ns

            # Immediate acquisition if enough tokens and no future start constraint
            if self.tokens >= tokens and effective_now_ns <= now_ns:
                self.tokens -= tokens
                return 0.0

            # Calculate exact time to wait until enough tokens are replenished
            needed = max(0.0, tokens - self.tokens)
            wait_seconds = needed / self.rate if self.rate > 0 else 0.0

            target_ns = max(self.last_update_ns, effective_now_ns) + int(wait_seconds * 1_000_000_000)
            self.tokens = 0.0
            self.last_update_ns = target_ns

            total_wait_sec = max(0.0, (target_ns - now_ns) / 1_000_000_000.0)

        # Sleep outside the lock so other coroutines can acquire slots concurrently
        if total_wait_sec > 0:
            await asyncio.sleep(total_wait_sec)

        return total_wait_sec * 1000.0


class AdaptiveRateLimiter:
    """
    Adaptive Rate Limiter that dynamically adjusts dispatch rate
    based on server feedback (HTTP 429, Retry-After, rate limit headers).
    """

    def __init__(
        self,
        base_rate: float = 10.0,
        burst_capacity: float = 15.0,
        min_rate: float = 0.5,
    ):
        self.base_rate = base_rate
        self.min_rate = min_rate
        self.current_rate = base_rate
        self.bucket = TokenBucketLimiter(rate=base_rate, capacity=burst_capacity)
        self.penalty_until_ns = 0
        self.consecutive_successes = 0

    async def wait_for_slot(self) -> float:
        """
        Waits if penalty is active or if token bucket is exhausted.
        Combines penalty backoff and token deficit into a single unified sleep,
        eliminating redundant double-sleep event loop wakeups.
        """
        return await self.bucket.acquire(1.0, min_start_ns=self.penalty_until_ns)

    def on_response(self, status_code: int, headers: Optional[Dict[str, Any]] = None):
        """
        Call this after every request to adapt the rate limiter based on server response.
        """
        headers = headers or {}
        now_ns = time.perf_counter_ns()

        # [FEATURE: ADAPTIVE_BACKOFF_503] 503 is treated like 429, and Retry-After accepts HTTP-dates.
        # Raison: overloaded ticketing servers answer 503, often with an HTTP-date Retry-After;
        #         the previous parser fell back to a fixed 2s and ignored 503 entirely.
        # Attention: penalty is capped at MAX_RETRY_AFTER_SEC to survive absurd header values.
        if status_code in THROTTLING_STATUS_CODES:
            self.consecutive_successes = 0
            retry_after_value = headers.get("retry-after") or headers.get("Retry-After")
            delay_sec = parse_retry_after_seconds(retry_after_value)
            if delay_sec is None:
                delay_sec = random.uniform(DEFAULT_PENALTY_MIN_SEC, DEFAULT_PENALTY_MAX_SEC)

            self.penalty_until_ns = max(self.penalty_until_ns, now_ns + int(delay_sec * 1_000_000_000))

            self.current_rate = max(self.min_rate, self.current_rate * RATE_CUT_FACTOR)
            self.bucket.rate = self.current_rate
            # Zero out remaining tokens and advance baseline to prevent burst after penalty
            self.bucket.tokens = 0.0
            self.bucket.last_update_ns = max(self.bucket.last_update_ns, self.penalty_until_ns)

        elif 200 <= status_code < 300:
            self.consecutive_successes += 1
            # Gradually restore rate if healthy
            if self.consecutive_successes > SUCCESSES_BEFORE_RECOVERY and self.current_rate < self.base_rate:
                self.current_rate = min(self.base_rate, self.current_rate * RATE_RECOVERY_FACTOR)
                self.bucket.rate = self.current_rate
                self.consecutive_successes = 0
