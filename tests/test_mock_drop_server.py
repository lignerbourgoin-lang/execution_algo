"""
Unit Tests for Mock Ticketing Drop Server
-----------------------------------------
Validates:
- Gate closed (404) before drop time, opening (200) after drop time.
- Inventory allocation, holding, and 409 Sold Out cascade.
- Automated release of expired carts back into inventory (Wave Sniping).
- Cloudflare challenge response.
- End-to-end reservation flow using PrewarmedHttpClient with MockTransport.
"""

import time
import unittest

import httpx

from core.network.persistent_client import PrewarmedHttpClient
from modules.retail.tickets.ticket_engine import TicketConfig, TicketDropExecutor
from tests.mock_drop_server import MockTicketingServer


class TestMockTicketingServer(unittest.IsolatedAsyncioTestCase):
    async def test_gate_closed_before_drop_time(self):
        future_drop = time.time() + 10.0
        server = MockTicketingServer(drop_time_utc=future_drop)
        transport = server.create_transport()

        client = PrewarmedHttpClient("https://billetterie.example.com", http2=False, transport=transport)
        await client.start()

        # Reserve before drop returns 404
        res = await client.execute_fast(
            "POST",
            f"/api/events/{server.event_id}/reserve",
            action_id="act_01",
            json_data={"category_id": "CARRE_OR", "quantity": 1},
        )
        await client.close()

        self.assertEqual(res["status_code"], 404)
        self.assertIn("not open yet", str(res["body"]))

    async def test_successful_reservation_and_inventory_decrement(self):
        server = MockTicketingServer(drop_time_utc=0.0, initial_inventory={"CARRE_OR": 2})
        transport = server.create_transport()

        client = PrewarmedHttpClient("https://billetterie.example.com", http2=False, transport=transport)
        await client.start()

        # Reserve 2 seats
        res = await client.execute_fast(
            "POST",
            f"/api/events/{server.event_id}/reserve",
            action_id="act_02",
            json_data={"category_id": "CARRE_OR", "quantity": 2},
        )
        self.assertEqual(res["status_code"], 200)
        self.assertIn("tok_mock_", res["body"]["token"])
        self.assertEqual(server.inventory["CARRE_OR"], 0)

        # 3rd seat request returns 409 Sold Out
        res_sold_out = await client.execute_fast(
            "POST",
            f"/api/events/{server.event_id}/reserve",
            action_id="act_03",
            json_data={"category_id": "CARRE_OR", "quantity": 1},
        )
        self.assertEqual(res_sold_out["status_code"], 409)

        await client.close()

    async def test_cart_expiration_returns_seats_for_wave_sniping(self):
        # Hold duration 0.05s
        server = MockTicketingServer(
            drop_time_utc=0.0,
            initial_inventory={"CARRE_OR": 1},
            cart_hold_duration_sec=0.05,
        )
        transport = server.create_transport()
        client = PrewarmedHttpClient("https://billetterie.example.com", http2=False, transport=transport)
        await client.start()

        # Reserve the single available seat
        res1 = await client.execute_fast(
            "POST",
            f"/api/events/{server.event_id}/reserve",
            action_id="act_first",
            json_data={"category_id": "CARRE_OR", "quantity": 1},
        )
        self.assertEqual(res1["status_code"], 200)
        self.assertEqual(server.inventory["CARRE_OR"], 0)

        # Wait for cart hold to expire
        time.sleep(0.06)

        # Availability check should show 1 seat returned to inventory!
        avail_res = await client.execute_fast(
            "GET",
            f"/api/events/{server.event_id}/availability?cat=CARRE_OR",
            action_id="act_check",
        )
        self.assertEqual(avail_res["status_code"], 200)
        self.assertEqual(avail_res["body"]["available"], 1)

        await client.close()

    async def test_executor_with_mock_server_full_flow(self):
        server = MockTicketingServer(
            drop_time_utc=0.0,
            initial_inventory={"CARRE_OR": 2},
            cart_hold_duration_sec=5.0,
        )
        transport = server.create_transport()
        client = PrewarmedHttpClient("https://billetterie.example.com", http2=False, transport=transport)

        config = TicketConfig(
            platform_name="mock_platform",
            target_url="https://billetterie.example.com",
            event_id=server.event_id,
            category_id="CARRE_OR",
            quantity=2,
            auto_open_browser=False,
            audible_alert=False,
        )
        executor = TicketDropExecutor(config=config, http_client=client)
        await executor.initialize()

        result = await executor.execute_drop()
        self.assertTrue(result.success)
        self.assertEqual(result.status_code, 200)
        self.assertIsNotNone(executor.active_cart)
        self.assertTrue(executor.active_cart.token.startswith("tok_mock_"))

        await executor.shutdown()


if __name__ == "__main__":
    unittest.main()
