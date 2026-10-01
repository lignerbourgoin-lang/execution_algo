"""
Staggered Multi-IP Wave Drop Orchestrator
-----------------------------------------
Coordinates multi-session reservation bursts with microsecond-staggered delay tiers:
- Groups IP sessions into temporal tiers (e.g. T0 + 0ms, T0 + 35ms, T0 + 75ms).
- Maximizes capture probability for non-deterministic server drop gates.
- First successful cart reservation instantly aborts pending executions (fail-safe quota).
- Integrates with IpCircuitBreakerPool to bypass burned or throttled proxies.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.engine.base import ExecutionResult
from core.network.circuit_breaker import IpCircuitBreakerPool, IpHealthState
from modules.retail.tickets.ticket_engine import TicketDropExecutor

logger = logging.getLogger("staggered_executor")


@dataclass
class StaggerConfig:
    stagger_interval_ms: float = 35.0
    sessions_per_tier: int = 4
    drop_time_utc: Optional[float] = None
    max_wait_sec: float = 10.0


@dataclass
class StaggeredDropResult:
    success: bool
    winning_result: Optional[ExecutionResult] = None
    winning_executor: Optional[TicketDropExecutor] = None
    winning_tier: int = -1
    total_dispatched: int = 0
    cancelled_count: int = 0
    duration_ms: float = 0.0
    error: Optional[str] = None


class StaggeredDropOrchestrator:
    """
    Orchestrates multi-IP ticket drop reservation attempts with tiered millisecond offsets.
    """

    def __init__(
        self,
        executors: List[TicketDropExecutor],
        config: Optional[StaggerConfig] = None,
        circuit_breaker: Optional[IpCircuitBreakerPool] = None,
    ) -> None:
        self.executors = executors
        self.config = config or StaggerConfig()
        self.circuit_breaker = circuit_breaker
        self._stop_event = asyncio.Event()

    async def execute_staggered_drop(self) -> StaggeredDropResult:
        """
        Executes reservation requests across all healthy executors in staggered tiers.
        First winning reservation terminates all other pending tiers.
        """
        start_monotonic = time.perf_counter()
        self._stop_event.clear()

        # 1. Filter viable executors using circuit breaker
        active_executors: List[TicketDropExecutor] = []
        for executor in self.executors:
            ip_candidate = getattr(executor.client, "bound_ip", None) or executor.config.target_url
            if self.circuit_breaker is not None and not self.circuit_breaker.is_available(ip_candidate):
                logger.info(f"Skipping executor on quarantined IP '{ip_candidate}'")
                continue
            active_executors.append(executor)

        if not active_executors:
            return StaggeredDropResult(
                success=False,
                error="No healthy executors available in circuit breaker pool",
            )

        # 2. Partition executors into tiers
        tier_size = max(1, self.config.sessions_per_tier)
        tiers: List[List[TicketDropExecutor]] = [
            active_executors[i : i + tier_size]
            for i in range(0, len(active_executors), tier_size)
        ]

        winning_result: Optional[ExecutionResult] = None
        winning_executor: Optional[TicketDropExecutor] = None
        winning_tier_index: int = -1
        total_dispatched = 0
        all_tasks: List[asyncio.Task] = []

        # [FEATURE: STAGGERED_TIMING_SYNC] Synchronize tier offsets relative to T0 target epoch
        # Raison: Prevents individual executors from re-waiting until T0 and collapsing all tiers into a single wave.
        # Attention: skip_scheduling=True avoids blocking the event loop on multiple simultaneous spin-waits.
        base_drop_utc = self.config.drop_time_utc
        if base_drop_utc is None and active_executors:
            base_drop_utc = getattr(active_executors[0].config, "drop_time_utc", None)

        # 3. Dispatch tiers with delay offsets
        async def run_single_executor(
            executor: TicketDropExecutor,
            tier_idx: int,
            tier_delay_sec: float,
        ) -> Optional[tuple[ExecutionResult, TicketDropExecutor, int]]:
            if base_drop_utc is not None and base_drop_utc > time.time():
                target_tier_epoch = base_drop_utc + tier_delay_sec
                remaining = target_tier_epoch - time.time()
                if remaining > 0:
                    try:
                        await asyncio.sleep(remaining)
                    except asyncio.CancelledError:
                        return None
            elif tier_delay_sec > 0:
                try:
                    await asyncio.sleep(tier_delay_sec)
                except asyncio.CancelledError:
                    return None

            if self._stop_event.is_set():
                return None

            try:
                try:
                    result = await executor.execute_drop(skip_scheduling=True)
                except TypeError:
                    result = await executor.execute_drop()

                ip_addr = getattr(executor.client, "bound_ip", None) or executor.config.target_url

                if result.success and executor.active_cart is not None:
                    if self.circuit_breaker is not None:
                        self.circuit_breaker.record_success(ip_addr)
                    return result, executor, tier_idx
                else:
                    if self.circuit_breaker is not None and result.status_code in (403, 429):
                        self.circuit_breaker.record_failure(
                            ip_address=ip_addr,
                            status_code=result.status_code,
                            error_message=str(result.error),
                        )
                    return None
            except asyncio.CancelledError:
                return None
            except Exception as task_error:
                logger.warning(f"Executor in tier {tier_idx} encountered unexpected error: {task_error}")
                return None

        # Launch all tiers
        for tier_index, tier_executors in enumerate(tiers):
            tier_delay_sec = (tier_index * self.config.stagger_interval_ms) / 1000.0
            for exec_instance in tier_executors:
                task = asyncio.create_task(
                    run_single_executor(exec_instance, tier_index, tier_delay_sec)
                )
                all_tasks.append(task)
                total_dispatched += 1

        # 4. Wait for first winner or timeout
        cancelled_count = 0
        try:
            for completed_task in asyncio.as_completed(all_tasks, timeout=self.config.max_wait_sec):
                try:
                    outcome = await completed_task
                    if outcome is not None:
                        res, exec_inst, t_idx = outcome
                        if res.success and winning_result is None:
                            winning_result = res
                            winning_executor = exec_inst
                            winning_tier_index = t_idx
                            self._stop_event.set()

                            # Cancel all other in-flight / pending tasks
                            for t in all_tasks:
                                if not t.done():
                                    t.cancel()
                                    cancelled_count += 1
                            break
                except asyncio.CancelledError:
                    pass
        except asyncio.TimeoutError:
            logger.warning("Staggered drop execution timed out.")
            for t in all_tasks:
                if not t.done():
                    t.cancel()
                    cancelled_count += 1

        total_duration_ms = (time.perf_counter() - start_monotonic) * 1000.0

        if winning_result is not None:
            return StaggeredDropResult(
                success=True,
                winning_result=winning_result,
                winning_executor=winning_executor,
                winning_tier=winning_tier_index,
                total_dispatched=total_dispatched,
                cancelled_count=cancelled_count,
                duration_ms=round(total_duration_ms, 2),
            )

        return StaggeredDropResult(
            success=False,
            total_dispatched=total_dispatched,
            cancelled_count=cancelled_count,
            duration_ms=round(total_duration_ms, 2),
            error="All staggered drop attempts failed or timed out",
        )
