"""
Core Engine Interfaces and Base Classes
---------------------------------------
Defines standardized contracts for:
- Signals: An actionable event detected by a monitor.
- Strategies: Logic that evaluates signals against risk/pricing rules.
- Executors: Module executing the actual order/request on target platform.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import time
from typing import Any, Dict, Optional


@dataclass
class Signal:
    """Standardized representation of a detected event/opportunity."""
    source: str
    target_id: str
    action: str  # e.g., "BUY", "RESERVE", "BID", "ACCEPT"
    payload: Dict[str, Any]
    detected_at_ns: int = field(default_factory=time.perf_counter_ns)
    urgency: int = 1  # 1 = normal, 2 = high, 3 = immediate/critical
    expires_at_ms: Optional[float] = None


@dataclass
class ExecutionResult:
    """Result of an executed action."""
    action_id: str
    success: bool
    status_code: int
    data: Dict[str, Any]
    latency_ms: float
    error: Optional[str] = None
    created_at_ns: int = field(default_factory=time.perf_counter_ns)


class BaseStrategy(ABC):
    """Abstract Strategy interface."""

    @abstractmethod
    def evaluate(self, market_data: Dict[str, Any]) -> Optional[Signal]:
        """Evaluates incoming streaming data and generates a Signal if conditions are met."""
        pass


class BaseExecutor(ABC):
    """Abstract Executor interface implemented by each vertical module."""

    @abstractmethod
    async def execute(self, signal: Signal) -> ExecutionResult:
        """Executes the action specified by the signal."""
        pass

    @abstractmethod
    async def initialize(self):
        """Pre-warms connections and sets up caches."""
        pass

    @abstractmethod
    async def shutdown(self):
        """Cleans up sockets and connections."""
        pass
