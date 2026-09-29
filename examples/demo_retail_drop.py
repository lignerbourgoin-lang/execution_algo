"""
Demonstration: Scheduled Execution at T0 with NTP Clock Sync
------------------------------------------------------------
Simulates:
1. NTP Clock synchronization against atomic time servers.
2. Pre-warming persistent HTTP connection to target API.
3. Scheduling execution at exact target timestamp T0 (with lead time RTT / 2).
4. Sub-millisecond dispatch and state machine checkout.
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from core.engine.base import Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.rate_limiter.limiter import AdaptiveRateLimiter
from core.telemetry.tracker import LatencyTracker
from modules.retail.checkout.state_machine import (
    CheckoutProfile,
    CheckoutState,
    FastCheckoutStateMachine,
)
from modules.retail.clock.ntp_sync import HighPrecisionScheduler, NtpClient

console = Console()


async def run_retail_demo():
    console.print(
        Panel(
            "[bold cyan]Démonstration : Exécution Programmée à T0 (NTP + Scheduler)[/bold cyan]\n"
            "• Synchronisation d'horloge atomique (NTP RFC 5905)\n"
            "• Déclenchement à la milliseconde exacte avec avance réseau (RTT / 2)\n"
            "• Machine à états de réservation avec données pré-sérialisées",
            title="[bold green]Retail & Drops Execution Engine[/bold green]",
            expand=False,
        )
    )

    # 1. NTP Synchronization
    console.print("[dim]Synchronisation avec les serveurs de temps atomique (Cloudflare, Google, NTP Pool)...[/dim]")
    ntp = NtpClient()
    sync_res = ntp.sync()

    table_ntp = Table(title="Résultats de Synchronisation d'Horloge NTP")
    table_ntp.add_column("Serveur NTP", style="bold white")
    table_ntp.add_column("Décalage d'Horloge (Offset)", justify="right", style="bold yellow")
    table_ntp.add_column("RTT Réseau", justify="right", style="cyan")
    table_ntp.add_column("Stratum", justify="center")

    for s in sync_res.get("details", []):
        table_ntp.add_row(
            s["server"],
            f"{s['offset_ms']:+.3f} ms",
            f"{s['rtt_ms']:.2f} ms",
            str(s["stratum"]),
        )

    console.print(table_ntp)
    console.print(
        f"[bold green][OK] Décalage médian retenu : {sync_res['median_offset_ms']:+.3f} ms[/bold green]\n"
    )

    # 2. Setup Pre-warmed HTTP Client & Checkout FSM
    telemetry = LatencyTracker()
    http_client = PrewarmedHttpClient(
        base_url="https://api.binance.com",  # Using live responsive endpoint for demonstration
        rate_limiter=AdaptiveRateLimiter(base_rate=5.0, burst_capacity=10.0),
        telemetry=telemetry,
    )

    profile = CheckoutProfile(
        email="customer@example.com",
        shipping_address={"country": "FR", "city": "Paris", "postal_code": "75001"},
    )

    fsm = FastCheckoutStateMachine(
        target_domain="https://api.binance.com",
        http_client=http_client,
        profile=profile,
        telemetry=telemetry,
    )

    console.print("[dim]Pré-chauffe de la socket TLS et armement de la machine à états...[/dim]")
    await fsm.initialize()
    console.print(f"[green][OK] Machine à états armée : Statut = {fsm.state.name}[/green]\n")

    # 3. Schedule T0 (e.g. 500 ms in future)
    scheduler = HighPrecisionScheduler(ntp)
    lead_time_ms = 10.0  # 10ms network advance
    target_t0 = ntp.get_atomic_time() + 0.500  # Exactly 500ms from now

    console.print(
        f"[bold yellow]>> Compte à rebours : Déclenchement programmé à T0 ({target_t0:.3f} UTC) avec {lead_time_ms} ms d'avance réseau...[/bold yellow]"
    )

    dispatch_metrics = await scheduler.wait_until_atomic_timestamp(
        target_atomic_timestamp_utc=target_t0,
        latency_advance_ms=lead_time_ms,
    )

    console.print(
        f"[bold green]>> T0 ATTEINT ! Déclenchement de l'ordre d'achat immédiat... (Précision temporelle : {dispatch_metrics['accuracy_us']} µs)[/bold green]"
    )

    # 4. Trigger Execution
    signal = Signal(
        source="t0_scheduler",
        target_id="retail_store",
        action="RESERVE_ITEM",
        payload={
            "item_id": "limited_drop_001",
            "quantity": 1,
            "reserve_endpoint": "/api/v3/time",
            "reserve_method": "GET",
            "shipping_endpoint": "/api/v3/time",
            "shipping_method": "GET",
        },
        urgency=3,
    )

    res = await fsm.execute(signal)

    console.print(
        f"\n[bold green]Résultat d'exécution :[/bold green] "
        f"Statut FSM: [bold]{fsm.state.name}[/bold] | "
        f"Succès: [bold]{res.success}[/bold] | "
        f"Latence totale: [bold]{res.latency_ms:.2f} ms[/bold]\n"
    )

    await fsm.shutdown()


if __name__ == "__main__":
    asyncio.run(run_retail_demo())
