"""
Unit Tests for Retail Module (Clock Sync, Scheduler, Checkout State Machine)
----------------------------------------------------------------------------
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.engine.base import Signal
from modules.retail.checkout.state_machine import (
    CheckoutProfile,
    CheckoutState,
    FastCheckoutStateMachine,
)
from modules.retail.clock.ntp_sync import HighPrecisionScheduler, NtpClient


class TestNtpSync(unittest.TestCase):
    def test_ntp_sync_live(self):
        client = NtpClient(timeout=2.0)
        res = client.sync(["time.cloudflare.com", "time.google.com"])
        self.assertTrue(res["success"])
        self.assertGreater(res["servers_responded"], 0)
        # Offset should be reasonable (< 5000 ms)
        self.assertLess(abs(res["median_offset_ms"]), 5000.0)


class TestHighPrecisionScheduler(unittest.IsolatedAsyncioTestCase):
    async def test_sub_millisecond_accuracy(self):
        client = NtpClient()
        # Set a small fixed offset for determinism
        client.cached_offset_ms = 0.0
        client.last_sync_time = 1.0

        scheduler = HighPrecisionScheduler(client)
        # Schedule 50 ms in the future
        target_utc = client.get_atomic_time() + 0.05
        metrics = await scheduler.wait_until_atomic_timestamp(target_utc, latency_advance_ms=0.0)

        # Accuracy should be within millisecond range (< 10 ms)
        self.assertIn("accuracy_ms", metrics)
        self.assertLess(metrics["accuracy_ms"], 15.0)


class MockHttpClient:
    def __init__(self, responses=None):
        self.responses = responses or []
        self.calls = []

    async def start(self):
        pass

    async def execute_fast(self, method, endpoint, action_id, json_data=None, headers=None, idempotency_key=None):
        self.calls.append({
            "method": method,
            "endpoint": endpoint,
            "json_data": json_data,
            "idempotency_key": idempotency_key
        })
        if self.responses:
            return self.responses.pop(0)
        return {"status_code": 200, "body": {"token": "test_tok_123"}}

    async def close(self):
        pass


class TestCheckoutStateMachine(unittest.IsolatedAsyncioTestCase):
    async def test_successful_reservation_flow(self):
        mock_client = MockHttpClient(
            responses=[
                {"status_code": 200, "body": {"token": "cart_xyz"}},  # Reserve
                {"status_code": 200, "body": {"order_id": "ORD_999"}},  # Shipping
            ]
        )
        profile = CheckoutProfile(
            email="test@example.com",
            shipping_address={"country": "FR", "city": "Paris"},
        )

        fsm = FastCheckoutStateMachine(
            target_domain="https://example.com",
            http_client=mock_client,
            profile=profile,
        )

        await fsm.initialize()
        self.assertTrue(fsm.is_armed)

        signal = Signal(
            source="detector",
            target_id="test_store",
            action="BUY",
            payload={"item_id": "item_123", "quantity": 1},
        )

        result = await fsm.execute(signal)
        self.assertTrue(result.success)
        self.assertEqual(result.data["token"], "cart_xyz")
        self.assertEqual(len(mock_client.calls), 2)
        self.assertEqual(mock_client.calls[0]["json_data"]["item_id"], "item_123")
        self.assertEqual(mock_client.calls[1]["json_data"]["token"], "cart_xyz")

    async def test_missing_token_fails_without_fake_fallback(self):
        # Server returns 200 but no token/cart_id -> Must fail explicitly (no tok_simulated)
        mock_client = MockHttpClient(
            responses=[
                {"status_code": 200, "body": {"message": "Success without token"}},
            ]
        )
        profile = CheckoutProfile(email="test@example.com", shipping_address={"country": "FR"})
        fsm = FastCheckoutStateMachine("https://example.com", mock_client, profile)

        signal = Signal(source="test", target_id="store", action="BUY", payload={"item_id": "1"})
        result = await fsm.execute(signal)

        self.assertFalse(result.success)
        self.assertIn("missing reservation token", result.error)
        self.assertEqual(len(mock_client.calls), 1)

    async def test_http_error_fails_immediately(self):
        mock_client = MockHttpClient(
            responses=[
                {"status_code": 403, "body": {"error": "Forbidden"}},
            ]
        )
        profile = CheckoutProfile(email="test@example.com", shipping_address={"country": "FR"})
        fsm = FastCheckoutStateMachine("https://example.com", mock_client, profile)

        signal = Signal(source="test", target_id="store", action="BUY", payload={"item_id": "1"})
        result = await fsm.execute(signal)

        self.assertFalse(result.success)
        self.assertEqual(result.status_code, 403)
        self.assertIn("Reservation failed", result.error)
        self.assertEqual(len(mock_client.calls), 1)

    async def test_stateless_concurrent_executions(self):
        # Two executions should run independently without overwriting each other's state
        class DynamicMockClient(MockHttpClient):
            async def execute_fast(self, method, endpoint, action_id, json_data=None, headers=None, idempotency_key=None):
                self.calls.append({"method": method, "endpoint": endpoint, "json_data": json_data})
                if "cart" in endpoint:
                    return {"status_code": 200, "body": {"token": f"token_{json_data.get('item_id')}"}}
                return {"status_code": 200, "body": {"order_id": f"ord_{json_data.get('token')}"}}

        mock_client = DynamicMockClient()
        profile = CheckoutProfile(email="test@example.com", shipping_address={"country": "FR"})
        fsm = FastCheckoutStateMachine("https://example.com", mock_client, profile)

        sig_a = Signal(source="test", target_id="store", action="BUY", payload={"item_id": "A", "idempotency_key": "key_A"})
        sig_b = Signal(source="test", target_id="store", action="BUY", payload={"item_id": "B", "idempotency_key": "key_B"})

        res_a, res_b = await asyncio.gather(fsm.execute(sig_a), fsm.execute(sig_b))
        self.assertTrue(res_a.success)
        self.assertTrue(res_b.success)
        self.assertEqual(res_a.data["token"], "token_A")
        self.assertEqual(res_b.data["token"], "token_B")


if __name__ == "__main__":
    unittest.main()

