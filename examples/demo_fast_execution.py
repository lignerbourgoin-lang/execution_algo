"""
Live Demonstration: Fast Execution Engine & Pre-Warmed Sockets
--------------------------------------------------------------
Shows:
1. Socket Pre-Warming & Heartbeat in action.
2. Signal triggering & instant execution dispatch.
3. Microsecond latency breakdown via LatencyTracker.
4. Token Bucket rate limiter enforcement.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from core.engine.base import BaseExecutor, ExecutionResult, Signal
from core.engine.orchestrator import ExecutionOrchestrator
from core.network.persistent_client import PrewarmedHttpClient
from core.rate_limiter.limiter import AdaptiveRateLimiter
from core.telemetry.tracker import LatencyTracker

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

console = Console()


class LiveMarketExecutor(BaseExecutor):
    """Real HTTP Executor targeting public API endpoint."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.telemetry = LatencyTracker()
        self.client = PrewarmedHttpClient(
            base_url=self.base_url,
            heartbeat_interval_sec=15.0,
            rate_limiter=AdaptiveRateLimiter(base_rate=5.0, burst_capacity=10.0),
            telemetry=self.telemetry,
        )

    async def initialize(self):
        console.print("[dim]Pre-warming TLS socket to target...[/dim]")
        await self.client.start()
        console.print("[green][OK] TLS Socket pre-warmed & ready in memory.[/green]")

    async def execute(self, signal: Signal) -> ExecutionResult:
        endpoint = signal.payload.get("endpoint", "/api/v3/time")
        res = await self.client.execute_fast(
            method="GET",
            endpoint=endpoint,
            action_id=f"signal_{signal.urgency}",
        )
        return ExecutionResult(
            action_id=signal.target_id,
            success=(res.get("status_code", 0) == 200),
            status_code=res.get("status_code", 0),
            data=res.get("body", {}),
            latency_ms=res.get("network_ms", 0.0),
        )

    async def shutdown(self):
        await self.client.close()


async def run_demo():
    console.print(
        Panel(
            "[bold cyan]Démonstration du Moteur d'Exécution Haute Performance (Phase 2)[/bold cyan]\n"
            "• Sockets persistantes pré-chauffées (0ms de DNS / TCP / TLS)\n"
            "• Routage de signal asynchrone non-bloquant\n"
            "• Chronométrage à la microseconde",
            title="[bold green]Execution Algo Core Engine[/bold green]",
            expand=False,
        )
    )

    orchestrator = ExecutionOrchestrator()
    executor = LiveMarketExecutor(base_url="https://api.binance.com")
    orchestrator.register_executor("market_target", executor)

    await orchestrator.initialize()

    table = Table(title="Exécutions Déclenchées par Signal")
    table.add_column("Ordre / Signal", style="bold white")
    table.add_column("Action", style="cyan")
    table.add_column("Statut HTTP", justify="center")
    table.add_column("Latence Réseau Pure", justify="right", style="bold green")
    table.add_column("Temps Total Pipeline", justify="right", style="bold magenta")

    # Simulate 5 rapid signals dispatched into the pipeline
    console.print("\n[bold yellow]>> Déclenchement d'une rafale de signaux d'opportunité...[/bold yellow]\n")

    for i in range(1, 6):
        signal = Signal(
            source="signal_detector",
            target_id="market_target",
            action="CHECK_PRICE",
            payload={"endpoint": "/api/v3/time", "index": i},
            urgency=3,
        )
        result = await orchestrator.dispatch_signal(signal)
        if result:
            table.add_row(
                f"Signal #{i}",
                signal.action,
                f"[green]{result.status_code}[/green]" if result.success else f"[red]{result.status_code}[/red]",
                f"{result.latency_ms} ms",
                f"{round(executor.telemetry.traces[-1].total_latency_ms, 2)} ms",
            )
        await asyncio.sleep(0.05)

    console.print(table)

    summary = executor.telemetry.get_summary()
    console.print(
        f"\n[bold green]Bilan Télémétrique :[/bold green] "
        f"Moyenne: [bold]{summary['avg_ms']} ms[/bold] | "
        f"Min: [bold]{summary['min_ms']} ms[/bold] | "
        f"Max: [bold]{summary['max_ms']} ms[/bold] | "
        f"Taux de succès: [bold]{summary['success_runs']}/{summary['total_runs']}[/bold]\n"
    )

    await orchestrator.shutdown()


if __name__ == "__main__":
    asyncio.run(run_demo())
