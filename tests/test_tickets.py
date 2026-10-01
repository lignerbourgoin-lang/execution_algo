"""
Unit Tests for Ticketing Drop & Cart Release Module
---------------------------------------------------
"""

import asyncio
import os
import sys
import time
import unittest
import httpx

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

    def build_fast_request(self, method, endpoint, json_data=None, content=None, headers=None, idempotency_key=None):
        return httpx.Request(
            method=method,
            url=f"https://billetterie.example.com{endpoint}",
            headers=headers or {},
        )

    async def send_fast(self, request, action_id):
        self.calls.append({
            "method": request.method,
            "endpoint": str(request.url),
            "action_id": action_id,
            "headers": dict(request.headers),
        })
        return self.reservation_response

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
        headers_lower = {k.lower(): v for k, v in req["headers"].items()}
        self.assertIn("authorization", headers_lower)
        self.assertIn("cookie", headers_lower)
        self.assertIn("session_id=sess_abc", headers_lower["cookie"])

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

    async def test_cascading_category_fallback(self):
        # Tier 1 (CARRE_OR) returns 409 Conflict (sold out)
        # Tier 2 (CAT_1) succeeds with 200
        class TieredMockClient(MockTicketingHttpClient):
            async def send_fast(self, request, action_id):
                self.calls.append({"endpoint": str(request.url), "action_id": action_id})
                if "CARRE_OR" in action_id:
                    return {"status_code": 409, "error": "Category CARRE_OR sold out"}
                return {
                    "status_code": 200,
                    "body": {"token": "tok_fallback_cat1", "hold_time_sec": 300},
                }

        mock_client = TieredMockClient()
        mock_ntp = MockNtpClient()
        config = TicketConfig(
            platform_name="billetterie_test",
            target_url="https://billetterie.example.com",
            event_id="CONCERT-2026",
            category_id="CARRE_OR",
            fallback_categories=["CAT_1", "FOSSE"],
            quantity=1,
            auto_open_browser=False,
            audible_alert=False,
        )

        executor = TicketDropExecutor(config=config, http_client=mock_client, ntp_client=mock_ntp)
        await executor.initialize()

        result = await executor.execute_drop()
        self.assertTrue(result.success)
        self.assertEqual(executor.active_cart.category_id, "CAT_1")
        self.assertEqual(executor.active_cart.token, "tok_fallback_cat1")
        # Ensure 2 attempts were made
        self.assertEqual(len(mock_client.calls), 2)


if __name__ == "__main__":
    unittest.main()
