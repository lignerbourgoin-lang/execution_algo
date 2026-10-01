"""
Unit Tests for Ticketing Drop & Cart Release Module
---------------------------------------------------
"""

import asyncio
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from modules.retail.tickets.ticket_engine import (
    CartReservation,
    TicketConfig,
    TicketDropExecutor,
)


class MockTicketingHttpClient:
    def __init__(self, reservation_response=None, availability_responses=None):
        self.reservation_response = reservation_response or {
            "status_code": 200,
            "body": {
                "token": "tok_ticket_cart_999",
                "checkout_url": "https://billetterie.example.com/checkout?cart=tok_ticket_cart_999",
                "hold_time_sec": 600,
            },
        }
        self.availability_responses = availability_responses or []
        self.calls = []

    async def start(self):
        pass

    async def execute_fast(self, method, endpoint, action_id, json_data=None, headers=None, idempotency_key=None):
        self.calls.append({
            "method": method,
            "endpoint": endpoint,
            "action_id": action_id,
            "json_data": json_data,
            "headers": headers,
            "idempotency_key": idempotency_key,
        })
        if "availability" in endpoint and self.availability_responses:
            return self.availability_responses.pop(0)
        return self.reservation_response

    async def close(self):
        pass


class MockNtpClient:
    def __init__(self):
        self.cached_offset_ms = 0.0
        self.last_sync_time = 100.0

    async def sync_async(self, servers=None):
        return {"success": True, "median_offset_ms": 0.0}

    def get_atomic_time(self):
        return time.time()


class TestTicketDropEngine(unittest.IsolatedAsyncioTestCase):
    async def test_ticket_reservation_flow(self):
        mock_client = MockTicketingHttpClient()
        mock_ntp = MockNtpClient()
        config = TicketConfig(
            platform_name="billetterie_test",
            target_url="https://billetterie.example.com",
            event_id="STADE-DE-FRANCE-2026",
            category_id="CARRE_OR",
            quantity=2,
            auth_token="jwt_secret_token_123",
            session_cookies={"session_id": "sess_abc"},
            auto_open_browser=False,  # Don't open browser during unit test
        )

        executor = TicketDropExecutor(config=config, http_client=mock_client, ntp_client=mock_ntp)
        await executor.initialize()
        self.assertTrue(executor.is_armed)

        result = await executor.execute_drop()
        self.assertTrue(result.success)
        self.assertIsNotNone(executor.active_cart)
        self.assertEqual(executor.active_cart.token, "tok_ticket_cart_999")
        self.assertEqual(executor.active_cart.quantity, 2)
        self.assertEqual(executor.active_cart.category_id, "CARRE_OR")
        self.assertIn("checkout?cart=", executor.active_cart.checkout_url)

        # Verify idempotency key and headers were passed
        self.assertGreater(len(mock_client.calls), 0)
        req = mock_client.calls[-1]
        self.assertIn("Authorization", req["headers"])
        self.assertIn("Cookie", req["headers"])
        self.assertIn("session_id=sess_abc", req["headers"]["Cookie"])
        self.assertIsNotNone(req["idempotency_key"])

    async def test_cart_release_sniping(self):
        # First 2 checks: 0 seats available. 3rd check: 2 seats released back into pool!
        mock_client = MockTicketingHttpClient(
            availability_responses=[
                {"status_code": 200, "body": {"available": 0}},
                {"status_code": 200, "body": {"available": 0}},
                {"status_code": 200, "body": {"available": 2}},
            ]
        )
        mock_ntp = MockNtpClient()
        config = TicketConfig(
            platform_name="billetterie_test",
            target_url="https://billetterie.example.com",
            event_id="STADE-DE-FRANCE-2026",
            category_id="CARRE_OR",
            quantity=2,
            auto_open_browser=False,
        )

        executor = TicketDropExecutor(config=config, http_client=mock_client, ntp_client=mock_ntp)
        await executor.initialize()

        res = await executor.monitor_cart_releases(poll_interval_sec=0.01, max_duration_sec=1.0)
        self.assertIsNotNone(res)
        self.assertTrue(res.success)
        self.assertIsNotNone(executor.active_cart)
        self.assertEqual(executor.active_cart.token, "tok_ticket_cart_999")


if __name__ == "__main__":
    unittest.main()
