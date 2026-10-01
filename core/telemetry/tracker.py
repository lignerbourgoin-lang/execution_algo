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

    def to_dict(self) -> Dict[str, Any]:
        """Serializes execution trace into a dictionary suitable for JSON serialization."""
        return {
            "action_id": self.action_id,
            "target": self.target,
            "started_at_ns": self.started_at_ns,
            "completed_at_ns": self.completed_at_ns,
            "total_latency_ms": round(self.total_latency_ms, 3),
            "success": self.success,
            "error": self.error,
            "metadata": self.metadata,
            "stages": [{"name": stage.name, "timestamp_ns": stage.timestamp_ns} for stage in self.stages],
            "latency_breakdown": self.get_breakdown(),
        }


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
            return {"total_runs": len(self.traces), "success_runs": 0, "avg_ms": 0.0}

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

    def export_audit_json(self, destination_path: str) -> str:
        """
        Exports all recorded traces and aggregated statistics to a formatted JSON audit file.
        Creates parent directories if necessary.
        """
        import json
        import os

        parent_dir = os.path.dirname(os.path.abspath(destination_path))
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        audit_payload = {
            "summary": self.get_summary(),
            "traces": [trace.to_dict() for trace in self.traces],
        }
        with open(destination_path, "w", encoding="utf-8") as json_file:
            json.dump(audit_payload, json_file, indent=2)

        return destination_path

    # [FEATURE: PREFLIGHT_SLA_CHECK] Automated pre-T0 SLA verification
    # Raison: Prevents firing a drop when proxy latency has degraded or clock drift is unacceptable.
    # Attention: Returns a dict with passed (bool), violations (list), and measured statistics.
    def check_preflight_sla(
        self,
        p95_threshold_ms: float = 120.0,
        clock_offset_ms: Optional[float] = None,
        max_clock_offset_ms: float = 50.0,
    ) -> Dict[str, Any]:
        """
        Validates whether prewarm traces and clock synchronization satisfy firing SLA requirements.
        """
        summary = self.get_summary()
        violations: List[str] = []

        measured_p95 = summary.get("p95_ms", 0.0)
        if measured_p95 > p95_threshold_ms:
            violations.append(
                f"Network latency p95 ({measured_p95:.1f} ms) exceeds SLA threshold ({p95_threshold_ms:.1f} ms)"
            )

        if clock_offset_ms is not None and abs(clock_offset_ms) > max_clock_offset_ms:
            violations.append(
                f"Clock drift ({abs(clock_offset_ms):.1f} ms) exceeds tolerance ({max_clock_offset_ms:.1f} ms)"
            )

        return {
            "passed": (len(violations) == 0),
            "p95_ms": measured_p95,
            "clock_offset_ms": clock_offset_ms,
            "violations": violations,
        }


