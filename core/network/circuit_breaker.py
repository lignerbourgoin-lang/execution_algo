"""
IP and Proxy Circuit Breaker
----------------------------
Manages the operational health of multi-IP pools and residential proxies:
- HEALTHY: Available for immediate dispatch.
- THROTTLED: Temporarily rate-limited (HTTP 429), cooling down.
- CHALLENGED: WAF challenge encountered (Cloudflare/DataDome), isolated for browser solving.
- BURNED: Hard failures (HTTP 403 or repeated timeouts), permanently quarantined for session.
"""

from __future__ import annotations

import enum
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class IpHealthState(enum.Enum):
    HEALTHY = "healthy"
    THROTTLED = "throttled"
    CHALLENGED = "challenged"
    BURNED = "burned"
    HALF_OPEN = "half_open"


@dataclass
class IpHealthRecord:
    ip_address: str
    state: IpHealthState = IpHealthState.HEALTHY
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    cooldown_until_epoch: float = 0.0
    last_status_code: int = 0
    last_error: str = ""
    total_requests: int = 0
    total_failures: int = 0


class IpCircuitBreakerPool:
    """
    Thread-safe circuit breaker pool tracking the viability of multiple IP addresses.
    Automatically quarantines burned or throttled IPs to protect critical drop timing.
    """

    def __init__(
        self,
        default_throttle_cooldown_sec: float = 5.0,
        default_challenge_cooldown_sec: float = 30.0,
        max_consecutive_failures_before_burn: int = 3,
    ) -> None:
        self.default_throttle_cooldown_sec = default_throttle_cooldown_sec
        self.default_challenge_cooldown_sec = default_challenge_cooldown_sec
        self.max_consecutive_failures_before_burn = max_consecutive_failures_before_burn
        self._records: Dict[str, IpHealthRecord] = {}
        self._lock = threading.Lock()

    def register_ip(self, ip_address: str) -> IpHealthRecord:
        """Registers an IP address into the circuit breaker pool if not already present."""
        with self._lock:
            if ip_address not in self._records:
                self._records[ip_address] = IpHealthRecord(ip_address=ip_address)
            return self._records[ip_address]

    def register_ips(self, ip_addresses: List[str]) -> None:
        """Batch registers a list of IP addresses into the circuit breaker pool."""
        with self._lock:
            for ip in ip_addresses:
                if ip not in self._records:
                    self._records[ip] = IpHealthRecord(ip_address=ip)

    def is_available(self, ip_address: str) -> bool:
        """
        Evaluates whether an IP address is viable for immediate request dispatch.
        Transition from THROTTLED/CHALLENGED to HALF_OPEN when cooldown expires.
        """
        with self._lock:
            record = self._records.get(ip_address)
            if record is None:
                # Unregistered IP defaults to healthy
                return True

            if record.state == IpHealthState.BURNED:
                return False

            now = time.time()
            if record.state in (IpHealthState.THROTTLED, IpHealthState.CHALLENGED):
                if now >= record.cooldown_until_epoch:
                    record.state = IpHealthState.HALF_OPEN
                    return True
                return False

            return True

    def get_available_ips(self, candidates: Optional[List[str]] = None) -> List[str]:
        """Returns the list of candidate IPs that are currently viable for dispatch."""
        with self._lock:
            target_ips = candidates if candidates is not None else list(self._records.keys())
            available_ips = []
            now = time.time()

            for ip in target_ips:
                record = self._records.get(ip)
                if record is None:
                    available_ips.append(ip)
                    continue

                if record.state == IpHealthState.BURNED:
                    continue

                if record.state in (IpHealthState.THROTTLED, IpHealthState.CHALLENGED):
                    if now >= record.cooldown_until_epoch:
                        record.state = IpHealthState.HALF_OPEN
                        available_ips.append(ip)
                    continue

                available_ips.append(ip)

            return available_ips

    def record_success(self, ip_address: str) -> None:
        """Records a successful HTTP response for the given IP, clearing failure streaks."""
        with self._lock:
            if ip_address not in self._records:
                self._records[ip_address] = IpHealthRecord(ip_address=ip_address)
            record = self._records[ip_address]

            record.total_requests += 1
            record.consecutive_successes += 1
            record.consecutive_failures = 0
            record.state = IpHealthState.HEALTHY
            record.cooldown_until_epoch = 0.0

    def record_failure(
        self,
        ip_address: str,
        status_code: int = 0,
        error_message: str = "",
        cooldown_sec: Optional[float] = None,
        is_challenge: bool = False,
    ) -> None:
        """
        Records a failure or throttling event for an IP address, adjusting its health state.
        """
        with self._lock:
            if ip_address not in self._records:
                self._records[ip_address] = IpHealthRecord(ip_address=ip_address)
            record = self._records[ip_address]

            now = time.time()
            record.total_requests += 1
            record.total_failures += 1
            record.consecutive_failures += 1
            record.consecutive_successes = 0
            record.last_status_code = status_code
            record.last_error = error_message

            # Case 1: WAF Challenge (Cloudflare, DataDome)
            if is_challenge:
                effective_cooldown = cooldown_sec or self.default_challenge_cooldown_sec
                record.state = IpHealthState.CHALLENGED
                record.cooldown_until_epoch = now + effective_cooldown
                return

            # Case 2: HTTP 429 Too Many Requests
            if status_code == 429:
                effective_cooldown = cooldown_sec or self.default_throttle_cooldown_sec
                record.state = IpHealthState.THROTTLED
                record.cooldown_until_epoch = now + effective_cooldown
                return

            # Case 3: HTTP 403 Forbidden without challenge or hard network disconnect
            if status_code == 403:
                # Outright ban or token rejection
                record.state = IpHealthState.BURNED
                return

            # Case 4: Repeated connection timeouts or socket resets
            if record.consecutive_failures >= self.max_consecutive_failures_before_burn:
                record.state = IpHealthState.BURNED
            else:
                # Temporary short cooldown for transient glitch
                transient_cooldown = cooldown_sec or 2.0
                record.state = IpHealthState.THROTTLED
                record.cooldown_until_epoch = now + transient_cooldown

    def reset_ip(self, ip_address: str) -> None:
        """Manually rehabilitates an IP address back to HEALTHY state."""
        with self._lock:
            if ip_address in self._records:
                record = self._records[ip_address]
                record.state = IpHealthState.HEALTHY
                record.consecutive_failures = 0
                record.cooldown_until_epoch = 0.0

    def get_summary(self) -> Dict[str, Any]:
        """Provides an aggregated health status report across all monitored IPs."""
        with self._lock:
            total_count = len(self._records)
            healthy_count = sum(1 for r in self._records.values() if r.state == IpHealthState.HEALTHY)
            throttled_count = sum(1 for r in self._records.values() if r.state == IpHealthState.THROTTLED)
            challenged_count = sum(1 for r in self._records.values() if r.state == IpHealthState.CHALLENGED)
            burned_count = sum(1 for r in self._records.values() if r.state == IpHealthState.BURNED)
            half_open_count = sum(1 for r in self._records.values() if r.state == IpHealthState.HALF_OPEN)

            return {
                "total_ips": total_count,
                "healthy": healthy_count,
                "throttled": throttled_count,
                "challenged": challenged_count,
                "burned": burned_count,
                "half_open": half_open_count,
            }
