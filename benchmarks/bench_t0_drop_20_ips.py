"""
Benchmark: T0 Drop Execution across 20 Concurrent Simulated IPs
---------------------------------------------------------------
Measures empirical baseline latency (p50, p95, min, max, jitter) at T0
using MockTicketingServer and PrewarmedHttpClient across 20 simulated IPs.
"""

from __future__ import annotations

import asyncio
import gc
import json
import os
import statistics
import sys
import time
from typing import Any, Dict, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.network.circuit_breaker import IpCircuitBreakerPool
from core.network.persistent_client import PrewarmedHttpClient
from core.system import acquire_timer_resolution, release_timer_resolution
from core.telemetry.tracker import LatencyTracker
from modules.retail.tickets.staggered_executor import (
    StaggerConfig,
    StaggeredDropOrchestrator,
)
from modules.retail.tickets.ticket_engine import (
    TicketConfig,
    TicketDropExecutor,
)
from tests.mock_drop_server import MockTicketingServer


async def setup_environment(
    num_ips: int = 20,
    inventory: Dict[str, int] | None = None,
    drop_time_utc: float = 0.0,
):
    if inventory is None:
        inventory = {"CARRE_OR": 100, "CAT_1": 200}

    server = MockTicketingServer(
        event_id="STADE-DE-FRANCE-2026",
        drop_time_utc=drop_time_utc,
        initial_inventory=inventory,
        cart_hold_duration_sec=10.0,
    )
    transport = server.create_transport()

    circuit_breaker = IpCircuitBreakerPool(default_throttle_cooldown_sec=2.0)
    candidate_ips = [f"192.168.10.{i}" for i in range(1, num_ips + 1)]
    circuit_breaker.register_ips(candidate_ips)

    shared_telemetry = LatencyTracker()
    executors: list[TicketDropExecutor] = []

    for ip in candidate_ips:
        client = PrewarmedHttpClient(
            base_url="https://billetterie.example.com",
            http2=False,
            transport=transport,
            headers={"X-Forwarded-For": ip},
            telemetry=shared_telemetry,
        )
        setattr(client, "bound_ip", ip)

        config = TicketConfig(
            platform_name="billetterie_sim",
            target_url="https://billetterie.example.com",
            event_id="STADE-DE-FRANCE-2026",
            category_id="CARRE_OR",
            quantity=1,
            auto_open_browser=False,
            audible_alert=False,
        )
        executor = TicketDropExecutor(
            config=config,
            http_client=client,
            telemetry=shared_telemetry,
        )
        await executor.initialize()
        executors.append(executor)

    return server, circuit_breaker, executors, shared_telemetry


async def benchmark_simultaneous_burst(num_runs: int = 30, warmup_runs: int = 5):
    """
    Measures 20 IPs firing concurrently at the exact same instant T0 (zero stagger).
    All 20 IPs compete simultaneously.
    """
    all_latencies: List[float] = []
    run_p50s: List[float] = []
    run_p95s: List[float] = []
    run_mins: List[float] = []
    run_maxs: List[float] = []
    cpu_percentages: List[float] = []

    total_runs = warmup_runs + num_runs

    for run_idx in range(total_runs):
        is_warmup = run_idx < warmup_runs
        server, cb, executors, tel = await setup_environment(
            num_ips=20,
            inventory={"CARRE_OR": 100},
            drop_time_utc=0.0,
        )

        cpu_start = time.process_time()
        wall_start = time.perf_counter()

        # Fire all 20 simultaneously at T0
        results = await asyncio.gather(*(exec_inst.execute_drop() for exec_inst in executors))

        wall_duration = time.perf_counter() - wall_start
        cpu_duration = time.process_time() - cpu_start
        cpu_pct = (cpu_duration / max(wall_duration, 1e-6)) * 100.0

        run_lats = [res.latency_ms for res in results if res.success]

        # Shutdown clients
        for e in executors:
            await e.shutdown()

        if not is_warmup and run_lats:
            all_latencies.extend(run_lats)
            sorted_lats = sorted(run_lats)
            run_mins.append(sorted_lats[0])
            run_maxs.append(sorted_lats[-1])
            run_p50s.append(sorted_lats[len(sorted_lats) // 2])
            run_p95s.append(sorted_lats[int(0.95 * len(sorted_lats))])
            cpu_percentages.append(cpu_pct)

    sorted_all = sorted(all_latencies)
    p50 = statistics.median(sorted_all)
    p95 = sorted_all[int(0.95 * len(sorted_all))]
    min_lat = sorted_all[0]
    max_lat = sorted_all[-1]
    mean_lat = statistics.mean(sorted_all)
    stdev_lat = statistics.stdev(sorted_all) if len(sorted_all) > 1 else 0.0

    return {
        "scenario": "Simultaneous T0 Burst (20 Concurrent IPs, Full Inventory)",
        "total_requests": len(sorted_all),
        "min_ms": round(min_lat, 3),
        "max_ms": round(max_lat, 3),
        "mean_ms": round(mean_lat, 3),
        "p50_ms": round(p50, 3),
        "p95_ms": round(p95, 3),
        "jitter_stdev_ms": round(stdev_lat, 3),
        "avg_cpu_pct": round(statistics.mean(cpu_percentages), 1),
    }


async def benchmark_contention_burst(num_runs: int = 30, warmup_runs: int = 5):
    """
    Measures 20 IPs firing concurrently at T0 with extreme contention:
    Only 2 seats available, 18 IPs will be rejected with 409 Sold Out.
    Measures first-winner latency and rejection handling latency.
    """
    winning_latencies: List[float] = []
    rejected_latencies: List[float] = []
    total_runs = warmup_runs + num_runs

    for run_idx in range(total_runs):
        is_warmup = run_idx < warmup_runs
        server, cb, executors, tel = await setup_environment(
            num_ips=20,
            inventory={"CARRE_OR": 2},
            drop_time_utc=0.0,
        )

        results = await asyncio.gather(*(exec_inst.execute_drop() for exec_inst in executors))

        for res in results:
            if not is_warmup:
                if res.success:
                    winning_latencies.append(res.latency_ms)
                else:
                    rejected_latencies.append(res.latency_ms)

        for e in executors:
            await e.shutdown()

    sorted_win = sorted(winning_latencies)
    sorted_rej = sorted(rejected_latencies)

    win_p50 = statistics.median(sorted_win) if sorted_win else 0.0
    win_p95 = sorted_win[int(0.95 * len(sorted_win))] if sorted_win else 0.0
    win_min = sorted_win[0] if sorted_win else 0.0
    win_max = sorted_win[-1] if sorted_win else 0.0
    win_jitter = statistics.stdev(sorted_win) if len(sorted_win) > 1 else 0.0

    rej_p50 = statistics.median(sorted_rej) if sorted_rej else 0.0
    rej_p95 = sorted_rej[int(0.95 * len(sorted_rej))] if sorted_rej else 0.0
    rej_min = sorted_rej[0] if sorted_rej else 0.0
    rej_max = sorted_rej[-1] if sorted_rej else 0.0
    rej_jitter = statistics.stdev(sorted_rej) if len(sorted_rej) > 1 else 0.0

    return {
        "scenario": "High Contention T0 Burst (20 Concurrent IPs, 2 Seats)",
        "winning_stats": {
            "count": len(sorted_win),
            "min_ms": round(win_min, 3),
            "max_ms": round(win_max, 3),
            "p50_ms": round(win_p50, 3),
            "p95_ms": round(win_p95, 3),
            "jitter_stdev_ms": round(win_jitter, 3),
        },
        "rejected_stats": {
            "count": len(sorted_rej),
            "min_ms": round(rej_min, 3),
            "max_ms": round(rej_max, 3),
            "p50_ms": round(rej_p50, 3),
            "p95_ms": round(rej_p95, 3),
            "jitter_stdev_ms": round(rej_jitter, 3),
        },
    }


async def benchmark_staggered_orchestrator(num_runs: int = 30, warmup_runs: int = 5):
    """
    Measures StaggeredDropOrchestrator with 20 IPs partitioned across 5 tiers of 4 IPs (stagger=25ms).
    Measures time-to-first-win and task cancellation speed.
    """
    e2e_durations: List[float] = []
    winning_latencies: List[float] = []
    cancellations: List[int] = []

    total_runs = warmup_runs + num_runs

    for run_idx in range(total_runs):
        is_warmup = run_idx < warmup_runs
        server, cb, executors, tel = await setup_environment(
            num_ips=20,
            inventory={"CARRE_OR": 2},
            drop_time_utc=0.0,
        )

        stagger_config = StaggerConfig(stagger_interval_ms=25.0, sessions_per_tier=4)
        orchestrator = StaggeredDropOrchestrator(
            executors=executors,
            config=stagger_config,
            circuit_breaker=cb,
        )

        stagger_res = await orchestrator.execute_staggered_drop()

        for e in executors:
            await e.shutdown()

        if not is_warmup:
            e2e_durations.append(stagger_res.duration_ms)
            if stagger_res.winning_result:
                winning_latencies.append(stagger_res.winning_result.latency_ms)
            cancellations.append(stagger_res.cancelled_count)

    sorted_e2e = sorted(e2e_durations)
    sorted_win = sorted(winning_latencies)

    return {
        "scenario": "Staggered Wave Orchestration (20 IPs, 5 tiers of 4 IPs, stagger=25ms)",
        "total_runs": len(sorted_e2e),
        "e2e_duration_p50_ms": round(statistics.median(sorted_e2e), 3),
        "e2e_duration_p95_ms": round(sorted_e2e[int(0.95 * len(sorted_e2e))], 3),
        "e2e_duration_min_ms": round(sorted_e2e[0], 3),
        "e2e_duration_max_ms": round(sorted_e2e[-1], 3),
        "winning_latency_p50_ms": round(statistics.median(sorted_win), 3) if sorted_win else 0.0,
        "winning_latency_p95_ms": round(sorted_win[int(0.95 * len(sorted_win))], 3) if sorted_win else 0.0,
        "winning_latency_min_ms": round(sorted_win[0], 3) if sorted_win else 0.0,
        "winning_latency_max_ms": round(sorted_win[-1], 3) if sorted_win else 0.0,
        "avg_tasks_cancelled": round(statistics.mean(cancellations), 1),
    }


async def main():
    acquire_timer_resolution()
    print("=" * 70)
    print("BENCHMARK T0 MULTI-IP EXECUTION (20 CONCURRENT SIMULATED IPS)")
    print("=" * 70)

    print("\n[1/3] Benchmarking Simultaneous T0 Burst (20 Concurrent IPs, Full Inventory)...")
    res_burst = await benchmark_simultaneous_burst(num_runs=30, warmup_runs=5)
    print(json.dumps(res_burst, indent=2))

    print("\n[2/3] Benchmarking High Contention T0 Burst (20 Concurrent IPs, 2 Seats)...")
    res_contention = await benchmark_contention_burst(num_runs=30, warmup_runs=5)
    print(json.dumps(res_contention, indent=2))

    print("\n[3/3] Benchmarking Staggered Wave Orchestrator (20 IPs, 5 Tiers)...")
    res_stagger = await benchmark_staggered_orchestrator(num_runs=30, warmup_runs=5)
    print(json.dumps(res_stagger, indent=2))

    release_timer_resolution()

    # Save summary report to JSON
    report_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "reports", "baseline_t0_20ips_benchmark.json"))
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": time.time(),
                "simultaneous_burst": res_burst,
                "high_contention": res_contention,
                "staggered_orchestration": res_stagger,
            },
            f,
            indent=2,
        )
    print(f"\n[OK] Baseline benchmark report saved to: {report_file}")


if __name__ == "__main__":
    asyncio.run(main())
