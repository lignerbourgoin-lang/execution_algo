"""
Unit Tests for Marketplace Execution Modules (Feed, Filter Engine, Buyer Executor)
---------------------------------------------------------------------------------
"""

import asyncio
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.engine.base import Signal
from modules.marketplace.buyer_executor import BuyerProfile, MarketplaceBuyerExecutor
from modules.marketplace.feed_monitor import MarketplaceFeedMonitor
from modules.marketplace.filter_engine import FilterRule, MarketplaceFilterStrategy


class TestMarketplaceFeedMonitor(unittest.IsolatedAsyncioTestCase):
    async def test_deduplication_and_callback(self):
        monitor = MarketplaceFeedMonitor(marketplace_name="vinted", max_seen_cache=5)
        received_items = []

        async def on_new(item, ts):
            received_items.append(item)

        monitor.register_callback(on_new)

        batch_1 = [
            {"id": "item_1", "title": "Nike Dunk Low", "price": 80.0},
            {"id": "item_2", "title": "Adidas Samba", "price": 60.0},
        ]

        new_1 = await monitor.ingest_items(batch_1)
        self.assertEqual(len(new_1), 2)
        self.assertEqual(len(received_items), 2)

        # Ingest same items again + 1 new item
        batch_2 = [
            {"id": "item_1", "title": "Nike Dunk Low", "price": 80.0},
            {"id": "item_3", "title": "New Balance 550", "price": 90.0},
        ]

        new_2 = await monitor.ingest_items(batch_2)
        self.assertEqual(len(new_2), 1)
        self.assertEqual(new_2[0]["id"], "item_3")
        self.assertEqual(len(received_items), 3)

        stats = monitor.get_stats()
        self.assertEqual(stats["total_detected"], 3)
        self.assertEqual(stats["duplicates_ignored"], 1)


class TestMarketplaceFilterEngine(unittest.TestCase):
    def setUp(self):
        self.rule = FilterRule(
            min_price=50.0,
            max_price=150.0,
            include_keywords=["jordan", "dunk", "travis"],
            exclude_keywords=["cassé", "fake", "boite vide", "pour pièces"],
            min_seller_rating=4.2,
            min_seller_reviews=3,
            allowed_countries={"FR", "BE"},
            max_item_age_sec=300.0,
        )
        self.strategy = MarketplaceFilterStrategy(target_marketplace="vinted", rule=self.rule)

    def test_matching_item_generates_signal(self):
        item = {
            "id": "123456",
            "title": "Nike Jordan 1 Retro High",
            "description": "État neuf, jamais portée avec facture",
            "price": 120.0,
            "created_at_epoch": time.time() - 10.0,
            "seller": {
                "id": "user_99",
                "rating": 4.8,
                "review_count": 25,
                "country": "FR",
            },
        }

        signal = self.strategy.evaluate(item)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "BUY_ITEM")
        self.assertEqual(signal.payload["item_id"], "123456")
        self.assertEqual(signal.payload["price"], 120.0)
        self.assertEqual(signal.urgency, 3)

    def test_blacklisted_keyword_rejected(self):
        item = {
            "id": "123457",
            "title": "Nike Jordan 1 - Boite vide",
            "description": "Uniquement la boite vide",
            "price": 60.0,
            "seller": {"rating": 5.0, "review_count": 10, "country": "FR"},
        }
        self.assertIsNone(self.strategy.evaluate(item))

    def test_price_out_of_bounds_rejected(self):
        # Too cheap (< 50)
        item_cheap = {
            "id": "123458",
            "title": "Nike Jordan 1",
            "price": 25.0,
            "seller": {"rating": 5.0, "review_count": 10, "country": "FR"},
        }
        self.assertIsNone(self.strategy.evaluate(item_cheap))

        # Too expensive (> 150)
        item_expensive = {
            "id": "123459",
            "title": "Nike Jordan 1",
            "price": 250.0,
            "seller": {"rating": 5.0, "review_count": 10, "country": "FR"},
        }
        self.assertIsNone(self.strategy.evaluate(item_expensive))

    def test_seller_trust_rejected(self):
        # Rating too low (< 4.2)
        item_low_rating = {
            "id": "123460",
            "title": "Nike Jordan 1",
            "price": 100.0,
            "seller": {"rating": 3.5, "review_count": 50, "country": "FR"},
        }
        self.assertIsNone(self.strategy.evaluate(item_low_rating))

        # Disallowed country
        item_bad_country = {
            "id": "123461",
            "title": "Nike Jordan 1",
            "price": 100.0,
            "seller": {"rating": 5.0, "review_count": 50, "country": "US"},
        }
        self.assertIsNone(self.strategy.evaluate(item_bad_country))


class MockHttpClientForBuyer:
    def __init__(self):
        self.requests = []

    async def start(self):
        pass

    async def execute_fast(self, method, endpoint, action_id, json_data=None, headers=None, idempotency_key=None):
        self.requests.append({
            "method": method,
            "endpoint": endpoint,
            "action_id": action_id,
            "json_data": json_data,
            "headers": headers,
            "idempotency_key": idempotency_key,
        })
        return {
            "status_code": 200,
            "body": {"status": "SUCCESS", "order_id": "ORD_VINTED_777"},
        }

    async def close(self):
        pass


class TestMarketplaceBuyerExecutor(unittest.IsolatedAsyncioTestCase):
    async def test_instant_buy_dispatch(self):
        mock_http = MockHttpClientForBuyer()
        profile = BuyerProfile(
            user_token="auth_token_secret_xyz",
            shipping_address_id="addr_123",
            payment_method_id="card_456",
        )
        executor = MarketplaceBuyerExecutor(
            base_url="https://api.vinted.fr",
            http_client=mock_http,
            profile=profile,
        )

        signal = Signal(
            source="marketplace_vinted",
            target_id="item_99999",
            action="BUY_ITEM",
            payload={"item_id": "item_99999", "price": 75.0},
        )

        result = await executor.execute(signal)
        self.assertTrue(result.success)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.data["order_id"], "ORD_VINTED_777")

        self.assertEqual(len(mock_http.requests), 1)
        req = mock_http.requests[0]
        self.assertEqual(req["headers"]["Authorization"], "Bearer auth_token_secret_xyz")
        self.assertIsNotNone(req["idempotency_key"])
        self.assertEqual(req["json_data"]["item_id"], "item_99999")
        self.assertEqual(req["json_data"]["shipping_address_id"], "addr_123")


if __name__ == "__main__":
    unittest.main()
