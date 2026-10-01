"""
Automated Unit Tests for Core Engine Architecture
-------------------------------------------------
Validates:
- TokenBucketLimiter token dynamics
- AdaptiveRateLimiter throttling & recovery
- LatencyTracker microsecond breakdown
- ExecutionOrchestrator event pipeline
"""

import asyncio
import json
import sys
import os
import unittest
import httpx

# Ensure execution_algo is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.engine.base import BaseExecutor, BaseStrategy, ExecutionResult, Signal
from core.engine.orchestrator import ExecutionOrchestrator
from core.network.persistent_client import PersistentHttpClient
from core.network.ws_client import WebSocketClient
from core.rate_limiter.limiter import AdaptiveRateLimiter, TokenBucketLimiter
from core.telemetry.tracker import LatencyTracker


class TestTokenBucketLimiter(unittest.IsolatedAsyncioTestCase):
    async def test_burst_and_exhaustion(self):
        # 5 tokens max burst, 2 tokens per second refill
        limiter = TokenBucketLimiter(rate=2.0, capacity=5.0)

        # 5 instantaneous acquisitions
        for _ in range(5):
            self.assertTrue(limiter.try_acquire(1.0))

        # 6th should fail without waiting
        self.assertFalse(limiter.try_acquire(1.0))

    async def test_async_acquire_waiting(self):
        # 1 token per second, empty bucket
        limiter = TokenBucketLimiter(rate=10.0, capacity=1.0)
        self.assertTrue(limiter.try_acquire(1.0))

        # Acquire with wait
        wait_ms = await limiter.acquire(1.0)
        self.assertGreaterEqual(wait_ms, 50.0)  # ~100ms for 1 token at 10/s


class TestAdaptiveRateLimiter(unittest.TestCase):
    def test_backoff_on_429(self):
        limiter = AdaptiveRateLimiter(base_rate=10.0, burst_capacity=10.0)
        initial_rate = limiter.current_rate

        # Simulate 429 Too Many Requests
        limiter.on_response(status_code=429, headers={"Retry-After": "1"})

        # Rate should be cut in half
        self.assertLess(limiter.current_rate, initial_rate)
        self.assertGreater(limiter.penalty_until_ns, 0)

    def test_recovery_on_200(self):
        limiter = AdaptiveRateLimiter(base_rate=10.0, burst_capacity=10.0)
        limiter.on_response(status_code=429)
        throttled_rate = limiter.current_rate

        # Simulate consecutive successes
        for _ in range(25):
            limiter.on_response(status_code=200)

        # Rate should begin recovering toward base_rate
        self.assertGreater(limiter.current_rate, throttled_rate)


class TestLatencyTracker(unittest.TestCase):
    def test_trace_breakdown(self):
        tracker = LatencyTracker()
        trace = tracker.start_trace(action_id="act_001", target="https://example.com")

        trace.mark_stage("stage_1")
        trace.mark_stage("stage_2")
        trace.complete(success=True)

        breakdown = trace.get_breakdown()
        self.assertIn("stage_1", breakdown)
        self.assertIn("stage_2", breakdown)
        self.assertIn("total_ms", breakdown)
        self.assertGreaterEqual(trace.total_latency_ms, 0.0)

        summary = tracker.get_summary()
        self.assertEqual(summary["total_runs"], 1)
        self.assertEqual(summary["success_runs"], 1)


class MockStrategy(BaseStrategy):
    def evaluate(self, market_data):
        if market_data.get("price", 0) < 100:
            return Signal(
                source="mock_feed",
                target_id="test_domain",
                action="BUY",
                payload={"order_size": 1.5, "price": market_data["price"]},
            )
        return None


class MockExecutor(BaseExecutor):
    def __init__(self):
        self.executed_signals = []
        self.initialized = False

    async def initialize(self):
        self.initialized = True

    async def execute(self, signal: Signal) -> ExecutionResult:
        self.executed_signals.append(signal)
        return ExecutionResult(
            action_id="mock_exec_1",
            success=True,
            status_code=200,
            data={"status": "FILLED"},
            latency_ms=1.2,
        )

    async def shutdown(self):
        self.initialized = False


class TestExecutionOrchestrator(unittest.IsolatedAsyncioTestCase):
    async def test_pipeline_dispatch(self):
        orchestrator = ExecutionOrchestrator()
        strategy = MockStrategy()
        executor = MockExecutor()

        orchestrator.register_strategy(strategy)
        orchestrator.register_executor("test_domain", executor)

        await orchestrator.initialize()
        self.assertTrue(executor.initialized)

        # Trigger event with price 150 (no signal expected)
        await orchestrator.on_event({"price": 150}, event_received_ns=1000)
        await asyncio.sleep(0.01)
        self.assertEqual(len(executor.executed_signals), 0)

        # Trigger event with price 95 (BUY signal expected)
        await orchestrator.on_event({"price": 95}, event_received_ns=2000)
        await asyncio.sleep(0.05)
        self.assertEqual(len(executor.executed_signals), 1)
        self.assertEqual(executor.executed_signals[0].action, "BUY")
        self.assertEqual(executor.executed_signals[0].payload["price"], 95)

        await orchestrator.shutdown()
        self.assertFalse(executor.initialized)

    async def test_bounded_execution_history(self):
        orchestrator = ExecutionOrchestrator()
        # Append 1050 mock results directly to execution_history
        for i in range(1050):
            orchestrator.execution_history.append(
                ExecutionResult(
                    action_id=f"act_{i}",
                    success=True,
                    status_code=200,
                    data={},
                    latency_ms=1.0,
                )
            )
        # History must not exceed maxlen (1000)
        self.assertEqual(len(orchestrator.execution_history), 1000)
        # The oldest elements should have been dropped
        self.assertEqual(orchestrator.execution_history[0].action_id, "act_50")
        self.assertEqual(orchestrator.execution_history[-1].action_id, "act_1049")


class TestPersistentHttpClient(unittest.IsolatedAsyncioTestCase):
    async def test_idempotency_key_and_content_headers(self):
        recorded_requests = []

        def mock_handler(request: httpx.Request):
            recorded_requests.append(request)
            return httpx.Response(200, json={"status": "confirmed"})

        transport = httpx.MockTransport(mock_handler)
        mock_client = httpx.AsyncClient(transport=transport, base_url="https://api.test.com")

        client = PersistentHttpClient(
            base_url="https://api.test.com",
            client=mock_client,
        )

        res = await client.execute_fast(
            method="POST",
            endpoint="/orders",
            action_id="act_idem_1",
            json_data={"symbol": "BTCUSDT", "qty": 0.5},
            idempotency_key="unique-idempotency-key-12345",
        )

        self.assertEqual(res["status_code"], 200)
        self.assertEqual(len(recorded_requests), 1)
        req = recorded_requests[0]
        self.assertEqual(req.headers.get("Idempotency-Key"), "unique-idempotency-key-12345")
        self.assertIn("application/json", req.headers.get("content-type", ""))
        body = json.loads(req.content.decode("utf-8"))
        self.assertEqual(body["symbol"], "BTCUSDT")

        await client.close()


class TestWebSocketClientGapDetection(unittest.IsolatedAsyncioTestCase):
    async def test_sequence_gap_detection(self):
        ws_client = WebSocketClient("wss://stream.binance.com:9443/ws/test")
        detected_gaps = []

        async def on_gap(last_seq, new_seq):
            detected_gaps.append((last_seq, new_seq))

        ws_client.on_sequence_gap(on_gap)

        # 1. First event: seq 100
        ws_client._dispatch_message(json.dumps({"u": 100, "price": "60000"}), 1000)
        await asyncio.sleep(0.01)
        self.assertEqual(ws_client.last_sequence_id, 100)
        self.assertEqual(ws_client.gaps_detected, 0)
        self.assertEqual(len(detected_gaps), 0)

        # 2. Second event: seq 101 (contiguous -> no gap)
        ws_client._dispatch_message(json.dumps({"u": 101, "price": "60010"}), 2000)
        await asyncio.sleep(0.01)
        self.assertEqual(ws_client.last_sequence_id, 101)
        self.assertEqual(ws_client.gaps_detected, 0)

        # 3. Third event: seq 105 (gap of 3 events: 102, 103, 104)
        ws_client._dispatch_message(json.dumps({"u": 105, "price": "60050"}), 3000)
        await asyncio.sleep(0.02)
        self.assertEqual(ws_client.last_sequence_id, 105)
        self.assertEqual(ws_client.gaps_detected, 1)
        self.assertEqual(len(detected_gaps), 1)
        self.assertEqual(detected_gaps[0], (101, 105))


if __name__ == "__main__":
    unittest.main()
