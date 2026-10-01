"""
Unit Tests for Retail Module (Clock Sync, Scheduler, Checkout State Machine)
----------------------------------------------------------------------------
Offline by default. Set EXECUTION_ALGO_LIVE_TESTS=1 to also query real NTP servers.
"""

import asyncio
import os
import socket
import struct
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.engine.base import Signal
from modules.retail.checkout.state_machine import (
    CheckoutProfile,
    CheckoutState,
    FastCheckoutStateMachine,
)
from modules.retail.clock.ntp_sync import (
    NTP_EPOCH_DELTA_SEC,
    ClockSyncError,
    HighPrecisionScheduler,
    NtpClient,
)

LIVE_TESTS_ENABLED = os.environ.get("EXECUTION_ALGO_LIVE_TESTS") == "1"
FAKE_SERVER_OFFSET_SEC = 2.5


class FakeNtpServer:
    """Local UDP server answering like an NTP server whose clock is FAKE_SERVER_OFFSET_SEC ahead."""

    def __init__(self, offset_sec: float, echo_originate: bool = True, stratum: int = 1):
        self.offset_sec = offset_sec
        self.echo_originate = echo_originate
        self.stratum = stratum
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind(("127.0.0.1", 0))
        self.udp_socket.settimeout(0.2)
        self.port = self.udp_socket.getsockname()[1]
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def _ntp_parts(self, unix_seconds: float):
        ntp_seconds = unix_seconds + NTP_EPOCH_DELTA_SEC
        return int(ntp_seconds), int((ntp_seconds - int(ntp_seconds)) * 2**32)

    def _serve(self):
        while not self._stop_event.is_set():
            try:
                request_packet, client_address = self.udp_socket.recvfrom(1024)
            except socket.timeout:
                continue
            server_now = time.time() + self.offset_sec
            originate = struct.unpack("!II", request_packet[40:48]) if self.echo_originate else (0, 0)
            header_byte = (0 << 6) | (4 << 3) | 4  # LI=0, VN=4, mode=4 (server)
            response_packet = struct.pack(
                "!BBBb11I",
                header_byte, self.stratum, 4, -20,
                0, 0, 0, 0, 0,
                originate[0], originate[1],
                *self._ntp_parts(server_now),
                *self._ntp_parts(server_now),
            )
            self.udp_socket.sendto(response_packet, client_address)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._stop_event.set()
        self._thread.join()
        self.udp_socket.close()


class TestNtpClient(unittest.IsolatedAsyncioTestCase):
    async def test_offset_measured_against_local_server(self):
        with FakeNtpServer(FAKE_SERVER_OFFSET_SEC) as server:
            client = NtpClient(timeout=1.0, port=server.port)
            result = await client.sync_async(["127.0.0.1"])
        self.assertTrue(result["success"])
        self.assertAlmostEqual(result["offset_ms"], FAKE_SERVER_OFFSET_SEC * 1000.0, delta=20.0)
        self.assertTrue(client.is_synced)
        self.assertGreaterEqual(result["uncertainty_ms"], 0.0)

    def test_reply_with_wrong_originate_is_rejected(self):
        with FakeNtpServer(FAKE_SERVER_OFFSET_SEC, echo_originate=False) as server:
            client = NtpClient(timeout=1.0, port=server.port)
            self.assertIsNone(client.query_server("127.0.0.1"))

    def test_kiss_of_death_is_rejected(self):
        with FakeNtpServer(FAKE_SERVER_OFFSET_SEC, stratum=0) as server:
            client = NtpClient(timeout=1.0, port=server.port)
            self.assertIsNone(client.query_server("127.0.0.1"))

    @unittest.skipUnless(LIVE_TESTS_ENABLED, "live network test (EXECUTION_ALGO_LIVE_TESTS=1)")
    def test_ntp_sync_live(self):
        client = NtpClient(timeout=2.0)
        result = client.sync(["time.cloudflare.com", "time.google.com"])
        self.assertTrue(result["success"])
        self.assertLess(abs(result["offset_ms"]), 5000.0)


class TestHighPrecisionScheduler(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_error_is_small(self):
        client = NtpClient()
        client.cached_offset_ms = 0.0
        client.cached_uncertainty_ms = 5.0

        scheduler = HighPrecisionScheduler(client)
        target_utc = client.get_atomic_time() + 0.05
        metrics = await scheduler.wait_until_atomic_timestamp(target_utc, latency_advance_ms=0.0)

        self.assertLess(metrics["dispatch_error_us"], 5000.0)
        self.assertFalse(metrics["fired_late"])
        self.assertEqual(metrics["clock_uncertainty_ms"], 5.0)

    async def test_past_target_is_reported_late(self):
        client = NtpClient()
        client.cached_uncertainty_ms = 1.0
        metrics = await HighPrecisionScheduler(client).wait_until_atomic_timestamp(time.time() - 1.0)
        self.assertTrue(metrics["fired_late"])

    async def test_unsynced_clock_fails_closed(self):
        client = NtpClient(timeout=0.2, port=9)  # discard port: nobody answers
        client.DEFAULT_SERVERS = ["127.0.0.1"]
        with self.assertRaises(ClockSyncError):
            await HighPrecisionScheduler(client).wait_until_atomic_timestamp(time.time() + 1.0)


class MockHttpClient:
    def __init__(self, responses=None, delay_sec: float = 0.0):
        self.responses = list(responses or [])
        self.calls = []
        self.delay_sec = delay_sec

    async def start(self):
        pass

    async def execute_fast(self, method, endpoint, action_id, json_data=None, headers=None, idempotency_key=None):
        self.calls.append(
            {"method": method, "endpoint": endpoint, "json_data": json_data, "idempotency_key": idempotency_key}
        )
        if self.delay_sec:
            await asyncio.sleep(self.delay_sec)
        if self.responses:
            return self.responses.pop(0)
        return {"status_code": 200, "body": {"token": "test_tok_123"}}

    async def close(self):
        pass


def make_machine(mock_client):
    profile = CheckoutProfile(email="test@example.com", shipping_address={"country": "FR", "city": "Paris"})
    return FastCheckoutStateMachine(target_domain="https://example.com", http_client=mock_client, profile=profile)


def make_signal(**payload_overrides):
    payload = {"item_id": "item_123", "quantity": 1}
    payload.update(payload_overrides)
    return Signal(source="detector", target_id="test_store", action="BUY", payload=payload)


class TestCheckoutStateMachine(unittest.IsolatedAsyncioTestCase):
    async def test_successful_reservation_flow(self):
        mock_client = MockHttpClient(
            responses=[
                {"status_code": 200, "body": {"token": "cart_xyz"}},
                {"status_code": 201, "body": {"order_id": "ORD_999"}},
            ]
        )
        machine = make_machine(mock_client)
        await machine.initialize()
        self.assertEqual(machine.state, CheckoutState.ARMED)

        result = await machine.execute(make_signal())
        self.assertTrue(result.success)
        self.assertEqual(machine.state, CheckoutState.COMPLETED)
        self.assertEqual(mock_client.calls[0]["json_data"]["item_id"], "item_123")
        self.assertEqual(mock_client.calls[1]["json_data"]["token"], "cart_xyz")
        reserve_key = mock_client.calls[0]["idempotency_key"]
        shipping_key = mock_client.calls[1]["idempotency_key"]
        self.assertEqual(reserve_key.rsplit("-", 1)[0], shipping_key.rsplit("-", 1)[0])

    async def test_non_json_reservation_does_not_fake_a_token(self):
        mock_client = MockHttpClient(responses=[{"status_code": 200, "body": "<html>queue</html>"}])
        machine = make_machine(mock_client)
        await machine.initialize()

        result = await machine.execute(make_signal())
        self.assertFalse(result.success)
        self.assertEqual(machine.state, CheckoutState.FAILED)
        self.assertEqual(len(mock_client.calls), 1, "shipping must not be sent without a token")

    async def test_cookie_cart_without_token_is_allowed_when_configured(self):
        mock_client = MockHttpClient(
            responses=[{"status_code": 204, "body": ""}, {"status_code": 200, "body": {"ok": True}}]
        )
        machine = make_machine(mock_client)
        await machine.initialize()
        result = await machine.execute(make_signal(require_reservation_token=False))
        self.assertTrue(result.success)
        self.assertNotIn("token", mock_client.calls[1]["json_data"])

    async def test_no_second_purchase_after_success(self):
        mock_client = MockHttpClient()
        machine = make_machine(mock_client)
        await machine.initialize()
        self.assertTrue((await machine.execute(make_signal())).success)

        second_result = await machine.execute(make_signal())
        self.assertFalse(second_result.success)
        self.assertEqual(len(mock_client.calls), 2, "no request may leave after COMPLETED")

        machine.reset()
        self.assertTrue((await machine.execute(make_signal())).success)

    async def test_concurrent_triggers_run_a_single_checkout(self):
        mock_client = MockHttpClient(delay_sec=0.05)
        machine = make_machine(mock_client)
        await machine.initialize()

        results = await asyncio.gather(*(machine.execute(make_signal()) for _ in range(5)))
        self.assertEqual(sum(r.success for r in results), 1)
        self.assertEqual(len(mock_client.calls), 2)

    async def test_get_methods_send_no_body(self):
        mock_client = MockHttpClient()
        machine = make_machine(mock_client)
        await machine.initialize()
        await machine.execute(make_signal(reserve_method="get", shipping_method="GET"))
        self.assertIsNone(mock_client.calls[0]["json_data"])
        self.assertIsNone(mock_client.calls[1]["json_data"])


if __name__ == "__main__":
    unittest.main()
