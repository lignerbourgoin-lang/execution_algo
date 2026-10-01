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

    async def test_micro_burst_retry_on_unopened_gate(self):
        # Server hasn't opened gates yet at T0: returns 404, then 425, then 200 on attempt 3
        attempts = 0

        class UnopenedGateMockClient(MockTicketingHttpClient):
            async def send_fast(self, request, action_id):
                nonlocal attempts
                attempts += 1
                self.calls.append({"endpoint": str(request.url), "action_id": action_id})
                if attempts == 1:
                    return {"status_code": 404, "error": "Sale not started yet"}
                elif attempts == 2:
                    return {"status_code": 425, "error": "Too Early"}
                else:
                    return {
                        "status_code": 200,
                        "body": {"token": "tok_burst_success", "hold_time_sec": 600},
                    }

        mock_client = UnopenedGateMockClient()
        mock_ntp = MockNtpClient()
        config = TicketConfig(
            platform_name="billetterie_test",
            target_url="https://billetterie.example.com",
            event_id="STADE-2026",
            category_id="CARRE_OR",
            quantity=1,
            burst_retries=4,
            burst_interval_ms=10.0,
            auto_open_browser=False,
            audible_alert=False,
        )

        executor = TicketDropExecutor(config=config, http_client=mock_client, ntp_client=mock_ntp)
        await executor.initialize()

        result = await executor.execute_drop()
        self.assertTrue(result.success)
        self.assertEqual(executor.active_cart.token, "tok_burst_success")
        self.assertEqual(attempts, 3)

    async def test_parallel_category_hedging(self):
        # Concurrently reserves across multiple categories; first successful reservation wins
        class HedgedMockClient(MockTicketingHttpClient):
            async def send_fast(self, request, action_id):
                if "CARRE_OR" in action_id:
                    await asyncio.sleep(0.05)
                    return {"status_code": 409, "error": "Sold out"}
                else:
                    return {"status_code": 200, "body": {"token": "tok_hedged_cat1"}}

        mock_client = HedgedMockClient()
        mock_ntp = MockNtpClient()
        config = TicketConfig(
            platform_name="billetterie_test",
            target_url="https://billetterie.example.com",
            event_id="PARALLEL-2026",
            category_id="CARRE_OR",
            fallback_categories=["CAT_1"],
            quantity=1,
            auto_open_browser=False,
            audible_alert=False,
        )

        executor = TicketDropExecutor(config=config, http_client=mock_client, ntp_client=mock_ntp)
        await executor.initialize()

        res = await executor.execute_parallel_categories()
        self.assertTrue(res.success)
        self.assertEqual(executor.active_cart.token, "tok_hedged_cat1")

    async def test_multi_category_wave_release_sniping(self):
        # Primary category has 0 seats, but Fallback category has 2 seats available
        class MultiCatReleaseClient(MockTicketingHttpClient):
            async def execute_fast(self, method, endpoint, action_id, json_data=None, headers=None, idempotency_key=None):
                if "cat=CARRE_OR" in endpoint:
                    return {"status_code": 200, "body": {"available": 0}}
                elif "cat=CAT_1" in endpoint:
                    return {"status_code": 200, "body": {"available": 2}}
                return {"status_code": 200, "body": {"available": 0}}

            async def send_fast(self, request, action_id):
                return {"status_code": 200, "body": {"token": "tok_release_cat1"}}

        mock_client = MultiCatReleaseClient()
        mock_ntp = MockNtpClient()
        config = TicketConfig(
            platform_name="billetterie_test",
            target_url="https://billetterie.example.com",
            event_id="WAVE-2026",
            category_id="CARRE_OR",
            fallback_categories=["CAT_1"],
            quantity=2,
            auto_open_browser=False,
            audible_alert=False,
        )

        executor = TicketDropExecutor(config=config, http_client=mock_client, ntp_client=mock_ntp)
        await executor.initialize()

        res = await executor.monitor_cart_releases(poll_interval_sec=0.01, max_duration_sec=0.5, jitter_ms=0.0)
        self.assertIsNotNone(res)
        self.assertTrue(res.success)
        self.assertEqual(executor.active_cart.token, "tok_release_cat1")
        self.assertEqual(executor.active_cart.category_id, "CAT_1")


if __name__ == "__main__":
    unittest.main()

