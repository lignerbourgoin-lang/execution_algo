"""
Telemetry and High-Precision Latency Tracking
---------------------------------------------
Tracks execution latency with nanosecond precision across every pipeline stage:
- Signal detection
- Queue/Dispatch delay
- Network transmission
- Server response acknowledgment
"""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class StageTimestamp:
    name: str
    timestamp_ns: int


@dataclass
class ExecutionTrace:
    action_id: str
    target: str
    started_at_ns: int = field(default_factory=time.perf_counter_ns)
    stages: List[StageTimestamp] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    completed_at_ns: Optional[int] = None
    success: bool = False
    error: Optional[str] = None

    def mark_stage(self, stage_name: str):
        """Record the precise nanosecond when a milestone is reached."""
        self.stages.append(StageTimestamp(name=stage_name, timestamp_ns=time.perf_counter_ns()))

    def complete(self, success: bool = True, error: Optional[str] = None):
        """Mark execution as finished and freeze timer."""
        self.completed_at_ns = time.perf_counter_ns()
        self.success = success
        self.error = error

    @property
    def total_latency_ms(self) -> float:
        if self.completed_at_ns is None:
            return 0.0
        return (self.completed_at_ns - self.started_at_ns) / 1_000_000.0

    def get_breakdown(self) -> Dict[str, float]:
        """Returns step-by-step latency delta in milliseconds."""
        breakdown = {}
        last_t = self.started_at_ns
        for s in self.stages:
            delta_ms = (s.timestamp_ns - last_t) / 1_000_000.0
            breakdown[s.name] = round(delta_ms, 3)
            last_t = s.timestamp_ns

        if self.completed_at_ns is not None:
            final_delta_ms = (self.completed_at_ns - last_t) / 1_000_000.0
            breakdown["final_ack"] = round(final_delta_ms, 3)

        breakdown["total_ms"] = round(self.total_latency_ms, 3)
        return breakdown


class LatencyTracker:
    """Aggregates and reports execution latency statistics across executions."""

    def __init__(self):
        self.traces: List[ExecutionTrace] = []

    def start_trace(self, action_id: str, target: str, **metadata) -> ExecutionTrace:
        trace = ExecutionTrace(action_id=action_id, target=target, metadata=metadata)
        self.traces.append(trace)
        return trace

    def get_summary(self) -> Dict[str, Any]:
        successful_traces = [t for t in self.traces if t.success]
        if not successful_traces:
            return {"count": len(self.traces), "success_count": 0, "avg_latency_ms": 0.0}

        latencies = [t.total_latency_ms for t in successful_traces]
        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(0.95 * len(latencies))]

        return {
            "total_runs": len(self.traces),
            "success_runs": len(successful_traces),
            "min_ms": round(min(latencies), 3),
            "max_ms": round(max(latencies), 3),
            "p50_ms": round(p50, 3),
            "p95_ms": round(p95, 3),
            "avg_ms": round(sum(latencies) / len(latencies), 3),
        }
