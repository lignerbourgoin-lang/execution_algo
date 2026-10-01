"""
Unit Tests for Financial Execution Modules (TWAP, VWAP, Arbitrage, Order Router)
--------------------------------------------------------------------------------
"""

import asyncio
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.engine.base import BaseExecutor, ExecutionResult, Signal
from modules.finance.arbitrage import SpatialArbitrageStrategy, VenueQuote
from modules.finance.order_router import ExchangeCredentials, ExchangeOrderExecutor
from modules.finance.twap import TWAPConfig, TWAPExecutor, TWAPStrategy
from modules.finance.vwap import VWAPConfig, VWAPExecutor, VWAPStrategy


class MockOrderExecutor(BaseExecutor):
    def __init__(self, should_succeed=True):
        self.should_succeed = should_succeed
        self.calls = []

    async def initialize(self):
        pass

    async def execute(self, signal: Signal) -> ExecutionResult:
        self.calls.append(signal)
        return ExecutionResult(
            action_id=signal.payload.get("client_order_id", "test_id"),
            success=self.should_succeed,
            status_code=200 if self.should_succeed else 400,
            data={"orderId": 9999, "status": "FILLED" if self.should_succeed else "REJECTED"},
            latency_ms=1.5,
        )

    async def shutdown(self):
        pass


class TestTWAPExecution(unittest.IsolatedAsyncioTestCase):
    async def test_twap_slice_generation_and_execution(self):
        config = TWAPConfig(
            symbol="BTCUSDT",
            side="BUY",
            total_quantity=10.0,
            duration_seconds=100.0,
            slices=5,
            jitter_pct=0.10,
        )
        mock_exec = MockOrderExecutor()
        twap = TWAPExecutor(config, mock_exec)

        # 1. Validate slice structure
        self.assertEqual(len(twap.slices), 5)
        total_planned = sum(s.planned_qty for s in twap.slices)
        self.assertAlmostEqual(total_planned, 10.0, places=4)

        # 2. Execute all slices at price 60,000
        for s in twap.slices:
            res = await twap.execute_slice(s, current_market_price=60000.0)
            self.assertTrue(res.success)
            self.assertEqual(s.status, "FILLED")

        # 3. Verify summary
        summary = twap.get_summary()
        self.assertEqual(summary["symbol"], "BTCUSDT")
        self.assertEqual(summary["total_filled_qty"], 10.0)
        self.assertEqual(summary["fill_rate_pct"], 100.0)
        self.assertEqual(summary["average_fill_price"], 60000.0)
        self.assertEqual(summary["slippage_bps"], 0.0)
        self.assertEqual(len(mock_exec.calls), 5)

    async def test_twap_price_limit_guard(self):
        config = TWAPConfig(
            symbol="BTCUSDT",
            side="BUY",
            total_quantity=5.0,
            duration_seconds=50.0,
            slices=2,
            price_limit=50000.0,  # Max buy price 50,000
        )
        mock_exec = MockOrderExecutor()
        twap = TWAPExecutor(config, mock_exec)

        # Try to execute when market is 52,000 (above limit)
        res = await twap.execute_slice(twap.slices[0], current_market_price=52000.0)
        self.assertFalse(res.success)
        self.assertEqual(twap.slices[0].status, "SKIPPED_PRICE_LIMIT")
        self.assertEqual(len(mock_exec.calls), 0)


class TestVWAPExecution(unittest.IsolatedAsyncioTestCase):
    async def test_vwap_weighting_and_outperformance(self):
        # 3 slices with custom weights: 20%, 50%, 30%
        config = VWAPConfig(
            symbol="ETHUSDT",
            side="BUY",
            total_quantity=10.0,
            duration_seconds=60.0,
            slices=3,
            volume_profile=[0.2, 0.5, 0.3],
            max_participation_rate=0.20,
        )
        mock_exec = MockOrderExecutor()
        vwap = VWAPExecutor(config, mock_exec)

        self.assertAlmostEqual(vwap.slices[0].planned_qty, 2.0, places=4)
        self.assertAlmostEqual(vwap.slices[1].planned_qty, 5.0, places=4)
        self.assertAlmostEqual(vwap.slices[2].planned_qty, 3.0, places=4)

        # Simulate benchmark trades in the market
        vwap.record_market_trade(price=3000.0, volume=100.0)
        vwap.record_market_trade(price=3050.0, volume=100.0)
        # Market VWAP = (3000*100 + 3050*100) / 200 = 3025.0
        self.assertEqual(vwap.market_vwap, 3025.0)

        # Execute our slices at favorable price 3010.0
        for s in vwap.slices:
            await vwap.execute_slice(s, current_market_price=3010.0, interval_market_volume=100.0)

        summary = vwap.get_summary()
        self.assertEqual(summary["total_filled_qty"], 10.0)
        self.assertEqual(summary["execution_vwap"], 3010.0)
        # Bought at 3010 vs market VWAP 3025 -> Outperformed by (3025-3010)/3025 * 10000 ~= 49.58 bps
        self.assertGreater(summary["outperformance_bps"], 40.0)


class TestSpatialArbitrage(unittest.TestCase):
    def test_arbitrage_detection_with_net_profit(self):
        strategy = SpatialArbitrageStrategy(
            symbol="BTCUSDT",
            min_profit_bps=10.0,  # 10 bps minimum profit
            slippage_buffer_bps=2.0,
        )

        # Venue A: Ask = 60,000 (we can BUY here)
        q_a = VenueQuote(
            venue_id="binance",
            symbol="BTCUSDT",
            bid_price=59990.0,
            bid_qty=1.0,
            ask_price=60000.0,
            ask_qty=0.5,
            fee_rate=0.0010,
        )
        # Venue B: Bid = 60,300 (we can SELL here) -> Gross spread = 300 / 60000 = 50 bps
        q_b = VenueQuote(
            venue_id="kraken",
            symbol="BTCUSDT",
            bid_price=60300.0,
            bid_qty=0.8,
            ask_price=60310.0,
            ask_qty=1.0,
            fee_rate=0.0010,
        )

        strategy.update_quote(q_a)
        strategy.update_quote(q_b)

        signal = strategy.evaluate({})
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "ARBITRAGE_EXECUTE")
        self.assertEqual(signal.payload["buy_venue"], "binance")
        self.assertEqual(signal.payload["sell_venue"], "kraken")
        self.assertEqual(signal.payload["quantity"], 0.5)  # Capped by Venue A ask_qty
        # Net spread: ~50 bps - (10 bps fee A + 10 bps fee B + 2 bps slippage) = ~28 bps
        self.assertGreater(signal.payload["net_spread_bps"], 20.0)
        self.assertGreater(signal.payload["estimated_profit_usd"], 50.0)

    def test_stale_quotes_rejected(self):
        strategy = SpatialArbitrageStrategy(symbol="BTCUSDT", max_quote_age_ms=50.0)
        old_time = time.perf_counter_ns() - int(200 * 1_000_000)  # 200 ms ago

        q_a = VenueQuote("binance", "BTCUSDT", 59990.0, 1.0, 60000.0, 1.0, timestamp_ns=old_time)
        q_b = VenueQuote("kraken", "BTCUSDT", 60500.0, 1.0, 60510.0, 1.0)

        strategy.update_quote(q_a)
        strategy.update_quote(q_b)

        # Must return None because q_a is stale
        self.assertIsNone(strategy.evaluate({}))


class MockHttpClientForOrderRouter:
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
            "body": {"orderId": 12345, "status": "FILLED", "clientOrderId": action_id},
        }

    async def close(self):
        pass


class TestExchangeOrderExecutor(unittest.IsolatedAsyncioTestCase):
    async def test_order_dispatch_with_hmac_signing(self):
        mock_http = MockHttpClientForOrderRouter()
        creds = ExchangeCredentials(
            api_key="test_api_key_abc",
            api_secret="test_secret_123",
            exchange_name="binance",
        )
        executor = ExchangeOrderExecutor(
            base_url="https://api.binance.com",
            http_client=mock_http,
            credentials=creds,
        )

        signal = Signal(
            source="twap",
            target_id="BTCUSDT",
            action="BUY",
            payload={
                "symbol": "BTCUSDT",
                "side": "BUY",
                "quantity": 0.25,
                "price": 62000.0,
                "type": "LIMIT",
                "client_order_id": "test_twap_slice_01",
            },
        )

        result = await executor.execute(signal)
        self.assertTrue(result.success)
        self.assertEqual(len(mock_http.requests), 1)

        req = mock_http.requests[0]
        self.assertEqual(req["headers"]["X-MBX-APIKEY"], "test_api_key_abc")
        self.assertEqual(req["idempotency_key"], "test_twap_slice_01")
        # Check HMAC signature and timestamp injection
        json_data = req["json_data"]
        self.assertIn("signature", json_data)
        self.assertIn("timestamp", json_data)
        self.assertEqual(json_data["symbol"], "BTCUSDT")
        self.assertEqual(json_data["quantity"], 0.25)


if __name__ == "__main__":
    unittest.main()
