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
import sys
import os
import unittest

# Ensure execution_algo is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.engine.base import BaseExecutor, BaseStrategy, ExecutionResult, Signal
from core.engine.orchestrator import ExecutionOrchestrator
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


if __name__ == "__main__":
    unittest.main()
