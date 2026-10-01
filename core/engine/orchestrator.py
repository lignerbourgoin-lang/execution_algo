"""
Execution Orchestrator
----------------------
Coordinates event streams, evaluation strategies, and low-latency executors.
Dispatches signals asynchronously with zero blocking overhead.
"""

import asyncio
import collections
import logging
import time
from typing import Any, Dict, List, Optional, Set

from core.engine.base import BaseExecutor, BaseStrategy, ExecutionResult, Signal
from core.telemetry.tracker import LatencyTracker

logger = logging.getLogger("core.engine.orchestrator")


class ExecutionOrchestrator:
    """
    Central dispatcher coordinating:
    - Data ingress -> Strategy evaluation -> Immediate Executor dispatch.
    - Strong task reference tracking (prevents GC drop and unhandled exceptions).
    - Bounded execution history (prevents unbounded memory growth).
    """

    def __init__(self, telemetry: Optional[LatencyTracker] = None, max_history: int = 1000):
        self.telemetry = telemetry or LatencyTracker()
        self.strategies: List[BaseStrategy] = []
        self.executors: Dict[str, BaseExecutor] = {}
        self.execution_history = collections.deque(maxlen=max_history)
        self._active_tasks: Set[asyncio.Task] = set()
        self._is_running = False

    def register_strategy(self, strategy: BaseStrategy):
        self.strategies.append(strategy)

    def register_executor(self, target_domain: str, executor: BaseExecutor):
        self.executors[target_domain] = executor

    async def initialize(self):
        """Initializes all registered executors (pre-warming sockets)."""
        self._is_running = True
        for domain, executor in self.executors.items():
            await executor.initialize()

    async def on_event(self, event_data: Dict[str, Any], event_received_ns: int):
        """
        Called on every stream event. Evaluates strategies and triggers immediate dispatch.
        """
        if not self._is_running:
            return

        # 1. Strategy Evaluation
        for strategy in self.strategies:
            signal = strategy.evaluate(event_data)
            if signal:
                signal.detected_at_ns = event_received_ns
                # Keep strong task reference to prevent silent GC drop
                task = asyncio.create_task(self.dispatch_signal(signal))
                self._active_tasks.add(task)
                task.add_done_callback(self._active_tasks.discard)

    async def dispatch_signal(self, signal: Signal) -> Optional[ExecutionResult]:
        """Routes signal to the dedicated executor."""
        executor = self.executors.get(signal.target_id) or self.executors.get(signal.source)
        if not executor:
            logger.warning(f"No executor registered for target: {signal.target_id}")
            return None

        # Execute immediately
        result = await executor.execute(signal)
        self.execution_history.append(result)
        return result

    async def shutdown(self):
        """Gracefully shuts down all executors and awaits remaining active tasks."""
        self._is_running = False
        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
            self._active_tasks.clear()
        for executor in self.executors.values():
            await executor.shutdown()
