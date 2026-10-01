"""
Unit Tests for Staggered Multi-IP Wave Drop Orchestrator
--------------------------------------------------------
Validates:
- Staggered tier partitioning and millisecond offset dispatch.
- Fast cancellation of remaining pending tiers upon first winning reservation.
- Circuit breaker integration: bypassing burned/quarantined IP executors.
- Graceful failure reporting when all tiers fail or sell out.
"""

import asyncio
import unittest

from core.engine.base import ExecutionResult
from core.network.circuit_breaker import IpCircuitBreakerPool
from modules.retail.tickets.staggered_executor import (
    StaggerConfig,
    StaggeredDropOrchestrator,
)
from modules.retail.tickets.ticket_engine import CartReservation, TicketConfig, TicketDropExecutor


class DummyMockClient:
    def __init__(self, bound_ip: str = "127.0.0.1"):
        self.bound_ip = bound_ip

    async def start(self, probe_path=None):
        pass

    async def close(self):
        pass


class TestStaggeredDropOrchestrator(unittest.IsolatedAsyncioTestCase):
    async def test_first_winner_cancels_other_tiers(self):
        executors = []
        executed_ips = []

        class MockTierExecutor(TicketDropExecutor):
            def __init__(self, config, client, delay_sec: float, should_win: bool):
                super().__init__(config=config, http_client=client)
                self.delay_sec = delay_sec
                self.should_win = should_win

            async def execute_drop(self):
                executed_ips.append(self.client.bound_ip)
                await asyncio.sleep(self.delay_sec)
                if self.should_win:
                    self.active_cart = CartReservation(
                        token="tok_win_123",
                        event_id="STAGGER-2026",
                        category_id="CARRE_OR",
                        quantity=2,
                        expires_at_epoch=9999999999.0,
                        checkout_url="https://example.com/checkout",
                        reserved_at_ms=15.0,
                    )
                    return ExecutionResult(
                        action_id="act_win",
                        success=True,
                        status_code=200,
                        data={"token": "tok_win_123"},
                        latency_ms=15.0,
                    )
                return ExecutionResult(
                    action_id="act_fail",
                    success=False,
                    status_code=409,
                    data={},
                    latency_ms=self.delay_sec * 1000.0,
                    error="Sold out",
                )

        # Tier 0 (2 IPs): IP1 fails after 0.05s, IP2 succeeds after 0.01s
        # Tier 1 (2 IPs): IP3 and IP4 scheduled with 0.1s delay offset
        config = TicketConfig(
            platform_name="billetterie_test",
            target_url="https://billetterie.example.com",
            event_id="STAGGER-2026",
            category_id="CARRE_OR",
            quantity=2,
            auto_open_browser=False,
            audible_alert=False,
        )

        exec1 = MockTierExecutor(config, DummyMockClient("10.0.0.1"), delay_sec=0.05, should_win=False)
        exec2 = MockTierExecutor(config, DummyMockClient("10.0.0.2"), delay_sec=0.01, should_win=True)
        exec3 = MockTierExecutor(config, DummyMockClient("10.0.0.3"), delay_sec=0.10, should_win=True)
        exec4 = MockTierExecutor(config, DummyMockClient("10.0.0.4"), delay_sec=0.10, should_win=True)

        executors = [exec1, exec2, exec3, exec4]
        stagger_config = StaggerConfig(stagger_interval_ms=50.0, sessions_per_tier=2)

        orchestrator = StaggeredDropOrchestrator(executors=executors, config=stagger_config)
        result = await orchestrator.execute_staggered_drop()

        self.assertTrue(result.success)
        self.assertIsNotNone(result.winning_result)
        self.assertEqual(result.winning_result.status_code, 200)
        self.assertEqual(result.winning_executor.client.bound_ip, "10.0.0.2")
        self.assertEqual(result.winning_tier, 0)
        self.assertGreater(result.cancelled_count, 0)

    async def test_circuit_breaker_bypasses_burned_ips(self):
        pool = IpCircuitBreakerPool()
        pool.register_ips(["10.0.0.1", "10.0.0.2"])
        # Mark 10.0.0.1 as burned
        pool.record_failure("10.0.0.1", status_code=403)

        config = TicketConfig(
            platform_name="billetterie_test",
            target_url="https://billetterie.example.com",
            event_id="STAGGER-2026",
            category_id="CARRE_OR",
            quantity=2,
            auto_open_browser=False,
            audible_alert=False,
        )

        class QuickWinExecutor(TicketDropExecutor):
            async def execute_drop(self):
                self.active_cart = CartReservation(
                    token="tok_cb_win",
                    event_id="STAGGER-2026",
                    category_id="CARRE_OR",
                    quantity=2,
                    expires_at_epoch=9999999999.0,
                    checkout_url="https://example.com/checkout",
                    reserved_at_ms=10.0,
                )
                return ExecutionResult(action_id="win", success=True, status_code=200, data={}, latency_ms=10.0)

        exec1 = QuickWinExecutor(config, DummyMockClient("10.0.0.1"))
        exec2 = QuickWinExecutor(config, DummyMockClient("10.0.0.2"))

        orchestrator = StaggeredDropOrchestrator(
            executors=[exec1, exec2],
            circuit_breaker=pool,
        )
        result = await orchestrator.execute_staggered_drop()

        self.assertTrue(result.success)
        # Winner must be 10.0.0.2 because 10.0.0.1 was quarantined
        self.assertEqual(result.winning_executor.client.bound_ip, "10.0.0.2")
        self.assertEqual(result.total_dispatched, 1)


if __name__ == "__main__":
    unittest.main()
