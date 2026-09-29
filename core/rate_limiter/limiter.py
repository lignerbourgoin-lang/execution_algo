"""
High-Performance Rate Limiter
------------------------------
Implements:
1. TokenBucketLimiter: Microsecond token bucket supporting burst capacity and steady rate.
2. AdaptiveRateLimiter: Dynamic backoff reacting to HTTP 429 and Retry-After headers.
"""

import asyncio
import random
import time
from typing import Any, Dict, Optional


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

    def _refill(self):
        now_ns = time.perf_counter_ns()
        elapsed_sec = (now_ns - self.last_update_ns) / 1_000_000_000.0
        self.tokens = min(self.capacity, self.tokens + elapsed_sec * self.rate)
        self.last_update_ns = now_ns

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Non-blocking token check. Returns True if token was acquired immediately."""
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0) -> float:
        """
        Asynchronous acquisition.
        Calculates required wait time inside the lock, then sleeps outside
        the lock to prevent serializing other concurrent tasks.
        Returns the duration waited in milliseconds.
        """
        async with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0

            # Calculate exact time to wait until enough tokens are replenished
            needed = tokens - self.tokens
            wait_seconds = needed / self.rate
            # Reserve slot by pushing virtual baseline forward
            self.tokens = 0.0
            self.last_update_ns = max(self.last_update_ns, time.perf_counter_ns()) + int(wait_seconds * 1_000_000_000)

        # Sleep outside the lock so other coroutines can acquire slots concurrently
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)

        return wait_seconds * 1000.0


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
        """Waits if penalty is active or if token bucket is exhausted."""
        now_ns = time.perf_counter_ns()
        if now_ns < self.penalty_until_ns:
            delay_sec = (self.penalty_until_ns - now_ns) / 1_000_000_000.0
            await asyncio.sleep(delay_sec)

        return await self.bucket.acquire(1.0)

    def on_response(self, status_code: int, headers: Optional[Dict[str, Any]] = None):
        """
        Call this after every request to adapt the rate limiter based on server response.
        """
        headers = headers or {}
        now_ns = time.perf_counter_ns()

        if status_code == 429:
            self.consecutive_successes = 0
            # Check Retry-After header
            retry_after = headers.get("retry-after") or headers.get("Retry-After")
            if retry_after:
                try:
                    delay_sec = float(retry_after)
                except ValueError:
                    delay_sec = 2.0
            else:
                # Jittered exponential penalty
                delay_sec = random.uniform(1.0, 3.0)

            self.penalty_until_ns = now_ns + int(delay_sec * 1_000_000_000)

            # Throttle the bucket rate by 50%
            self.current_rate = max(self.min_rate, self.current_rate * 0.5)
            self.bucket.rate = self.current_rate

        elif 200 <= status_code < 300:
            self.consecutive_successes += 1
            # Gradually restore rate if healthy
            if self.consecutive_successes > 20 and self.current_rate < self.base_rate:
                self.current_rate = min(self.base_rate, self.current_rate * 1.1)
                self.bucket.rate = self.current_rate
                self.consecutive_successes = 0
