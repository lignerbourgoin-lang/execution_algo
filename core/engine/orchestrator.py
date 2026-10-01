"""
Execution Orchestrator
----------------------
Coordinates event streams, evaluation strategies, and low-latency executors.
Dispatches signals asynchronously with zero blocking overhead.
"""

import logging
from collections import deque
from typing import Any, Deque, Dict, List, Optional

from core.engine.base import BaseExecutor, BaseStrategy, ExecutionResult, Signal
from core.tasks import BackgroundTaskSet
from core.telemetry.tracker import LatencyTracker

EXECUTION_HISTORY_MAX_LENGTH = 10_000

logger = logging.getLogger("core.engine.orchestrator")


class ExecutionOrchestrator:
    """
    Central dispatcher coordinating:
    - Data ingress -> Strategy evaluation -> Immediate Executor dispatch.
    """

    def __init__(self, telemetry: Optional[LatencyTracker] = None):
        self.telemetry = telemetry or LatencyTracker()
        self.strategies: List[BaseStrategy] = []
        self.executors: Dict[str, BaseExecutor] = {}
        # Bounded: a long-running monitor must not grow memory without limit.
        self.execution_history: Deque[ExecutionResult] = deque(maxlen=EXECUTION_HISTORY_MAX_LENGTH)
        self._dispatch_tasks = BackgroundTaskSet(owner_name="orchestrator")
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
                # Non-blocking dispatch; the task set keeps a reference and logs failures.
                self._dispatch_tasks.spawn(self.dispatch_signal(signal), name=f"dispatch_{signal.target_id}")

    async def dispatch_signal(self, signal: Signal) -> Optional[ExecutionResult]:
        """Routes signal to the executor registered for its target_id (fallback: its source)."""
        executor = self.executors.get(signal.target_id) or self.executors.get(signal.source)
        if executor is None:
            logger.warning("No executor registered for target: %s", signal.target_id)
            return None

        # Execute immediately
        result = await executor.execute(signal)
        self.execution_history.append(result)
        return result

    async def shutdown(self):
        """Gracefully shuts down all executors."""
        self._is_running = False
        await self._dispatch_tasks.wait_all()
        for executor in self.executors.values():
            await executor.shutdown()
