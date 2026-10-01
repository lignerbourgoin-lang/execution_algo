"""
Unit Tests for IP Circuit Breaker Pool
--------------------------------------
Validates:
- Health state transitions (HEALTHY -> THROTTLED -> HALF_OPEN -> BURNED).
- Cooldown expiration and half-open state recovery.
- Consecutive failure thresholds and quarantine enforcement.
- Aggregated health summary metrics.
"""

import time
import unittest

from core.network.circuit_breaker import IpCircuitBreakerPool, IpHealthState


class TestIpCircuitBreakerPool(unittest.TestCase):
    def setUp(self):
        self.pool = IpCircuitBreakerPool(
            default_throttle_cooldown_sec=0.05,
            default_challenge_cooldown_sec=0.05,
            max_consecutive_failures_before_burn=3,
        )

    def test_new_ip_is_healthy_and_available(self):
        ip = "192.168.1.10"
        self.assertTrue(self.pool.is_available(ip))
        record = self.pool.register_ip(ip)
        self.assertEqual(record.state, IpHealthState.HEALTHY)

    def test_429_throttles_with_cooldown(self):
        ip = "192.168.1.11"
        self.pool.record_failure(ip, status_code=429, cooldown_sec=0.05)
        self.assertFalse(self.pool.is_available(ip))

        record = self.pool.register_ip(ip)
        self.assertEqual(record.state, IpHealthState.THROTTLED)

        # After cooldown, should become available and transition to HALF_OPEN
        time.sleep(0.06)
        self.assertTrue(self.pool.is_available(ip))
        self.assertEqual(record.state, IpHealthState.HALF_OPEN)

        # Successful response restores to HEALTHY
        self.pool.record_success(ip)
        self.assertEqual(record.state, IpHealthState.HEALTHY)

    def test_waf_challenge_marks_challenged(self):
        ip = "192.168.1.12"
        self.pool.record_failure(ip, status_code=403, is_challenge=True, cooldown_sec=0.05)
        record = self.pool.register_ip(ip)
        self.assertEqual(record.state, IpHealthState.CHALLENGED)
        self.assertFalse(self.pool.is_available(ip))

    def test_403_forbidden_burns_ip(self):
        ip = "192.168.1.13"
        self.pool.record_failure(ip, status_code=403, is_challenge=False)
        record = self.pool.register_ip(ip)
        self.assertEqual(record.state, IpHealthState.BURNED)
        self.assertFalse(self.pool.is_available(ip))

    def test_consecutive_transient_failures_burn_ip(self):
        ip = "192.168.1.14"
        self.pool.record_failure(ip, status_code=500, error_message="ConnectTimeout")
        self.assertEqual(self.pool.register_ip(ip).state, IpHealthState.THROTTLED)

        self.pool.record_failure(ip, status_code=500, error_message="ConnectTimeout")
        self.assertEqual(self.pool.register_ip(ip).state, IpHealthState.THROTTLED)

        # 3rd failure reaches max_consecutive_failures_before_burn
        self.pool.record_failure(ip, status_code=500, error_message="ConnectTimeout")
        self.assertEqual(self.pool.register_ip(ip).state, IpHealthState.BURNED)
        self.assertFalse(self.pool.is_available(ip))

    def test_get_available_ips_filters_burned(self):
        ip1 = "10.0.0.1"
        ip2 = "10.0.0.2"
        ip3 = "10.0.0.3"
        self.pool.register_ips([ip1, ip2, ip3])

        self.pool.record_failure(ip2, status_code=403)  # burned
        available = self.pool.get_available_ips([ip1, ip2, ip3])
        self.assertIn(ip1, available)
        self.assertNotIn(ip2, available)
        self.assertIn(ip3, available)

    def test_health_summary(self):
        self.pool.register_ips(["1.1.1.1", "2.2.2.2", "3.3.3.3"])
        self.pool.record_failure("2.2.2.2", status_code=429, cooldown_sec=10.0)
        self.pool.record_failure("3.3.3.3", status_code=403)

        summary = self.pool.get_summary()
        self.assertEqual(summary["total_ips"], 3)
        self.assertEqual(summary["healthy"], 1)
        self.assertEqual(summary["throttled"], 1)
        self.assertEqual(summary["burned"], 1)


if __name__ == "__main__":
    unittest.main()
