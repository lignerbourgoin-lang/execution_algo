"""
Demonstration: End-to-End Ticketing Drop Simulation
---------------------------------------------------
Simulates a real-world high-traffic ticketing drop:
1. MockTicketingServer with drop gate, inventory constraints, and rate limiting.
2. IpCircuitBreakerPool quarantining failing or challenged proxies.
3. StaggeredDropOrchestrator firing multi-IP waves at millisecond offsets.
4. Fast cancellation of pending tiers upon winning reservation.
5. Wave sniping detecting returned seats from expired carts.
6. JSON nanosecond audit trace export.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from core.network.circuit_breaker import IpCircuitBreakerPool, IpHealthState
from core.network.persistent_client import PrewarmedHttpClient
from core.network.waf_detector import detect_waf_challenge
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

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

console = Console()


async def run_simulation():
    console.print(
        Panel.fit(
            "[bold cyan]TICKETING DROP SIMULATOR[/bold cyan] : [bold green]Full Integration Engine[/bold green]\n"
            "[dim]Mock Server -> Circuit Breaker -> Staggered Wave T0 -> Wave Sniping[/dim]",
            border_style="cyan",
        )
    )

    # 1. Initialize Mock Server with limited inventory
    now = time.time()
    drop_time = now + 0.2  # Gate opens in 200 ms
    server = MockTicketingServer(
        event_id="STADE-DE-FRANCE-2026",
        drop_time_utc=drop_time,
        initial_inventory={"CARRE_OR": 2, "CAT_1": 4},
        cart_hold_duration_sec=1.5,  # 1.5s cart hold for fast wave demonstration
    )
    transport = server.create_transport()

    # Inject WAF challenge on IP 3 and 403 Ban on IP 4 to demonstrate Circuit Breaker
    server.trigger_cloudflare_challenge("192.168.10.3")
    server.trigger_ip_ban("192.168.10.4")

    # 2. Setup Circuit Breaker
    circuit_breaker = IpCircuitBreakerPool(default_throttle_cooldown_sec=2.0)
    candidate_ips = [f"192.168.10.{i}" for i in range(1, 9)]
    circuit_breaker.register_ips(candidate_ips)

    # 3. Create 8 multi-IP executors
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
            quantity=2,
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

    console.print(f"[bold green]>[/bold green] Prewarmed {len(executors)} sessions across multi-IP pool.")

    # Pre-flight health probe to detect any challenged or banned IPs
    console.print("[dim]> Running pre-flight proxy health probe...[/dim]")
    for exec_inst in executors:
        ip = getattr(exec_inst.client, "bound_ip", "")
        probe_res = await exec_inst.client.execute_fast("GET", f"/api/events/{server.event_id}/availability", action_id=f"probe_{ip}")
        waf_check = detect_waf_challenge(
            status_code=probe_res.get("status_code", 0),
            headers=probe_res.get("headers"),
            body_text_or_bytes=str(probe_res.get("body", "")),
        )
        if waf_check.is_blocked:
            circuit_breaker.record_failure(
                ip_address=ip,
                status_code=probe_res.get("status_code", 0),
                is_challenge=waf_check.is_interactive_challenge,
            )

    console.print(f"[bold green]>[/bold green] Gate opens at T0 in [yellow]{int((drop_time - time.time()) * 1000)} ms[/yellow]...")

    # Wait until just before drop time
    wait_time = max(0.0, drop_time - time.time())
    if wait_time > 0:
        await asyncio.sleep(wait_time)

    # 4. Execute Staggered Wave Drop
    stagger_config = StaggerConfig(stagger_interval_ms=25.0, sessions_per_tier=2)
    orchestrator = StaggeredDropOrchestrator(
        executors=executors,
        config=stagger_config,
        circuit_breaker=circuit_breaker,
    )

    console.print("[bold yellow]>[/bold yellow] [bold white]FIRING STAGGERED T0 BURST...[/bold white]")
    stagger_res = await orchestrator.execute_staggered_drop()

    if stagger_res.success and stagger_res.winning_executor:
        win_ip = getattr(stagger_res.winning_executor.client, "bound_ip", "Unknown")
        cart = stagger_res.winning_executor.active_cart
        console.print(
            Panel(
                f"[bold green]RESERVATION SECURED AT T0 ![/bold green]\n"
                f"[white]Winning IP       :[/white] [cyan]{win_ip}[/cyan]\n"
                f"[white]Winning Tier     :[/white] [yellow]Tier {stagger_res.winning_tier}[/yellow]\n"
                f"[white]Cart Token       :[/white] [bold green]{cart.token if cart else 'None'}[/bold green]\n"
                f"[white]Execution Latency:[/white] [yellow]{stagger_res.duration_ms:.2f} ms[/yellow]\n"
                f"[white]Tasks Cancelled  :[/white] [magenta]{stagger_res.cancelled_count} pending executions aborted[/magenta]",
                border_style="green",
            )
        )
    else:
        console.print(f"[red]Drop failed: {stagger_res.error}[/red]")

    # 5. Display Circuit Breaker Summary Table
    cb_table = Table(title="Circuit Breaker Status Report", header_style="bold cyan")
    cb_table.add_column("IP Address", style="white")
    cb_table.add_column("Health State", style="bold")
    cb_table.add_column("Consecutive Failures", justify="center")
    cb_table.add_column("Viable for Dispatch", justify="center")

    for ip in candidate_ips:
        record = circuit_breaker.register_ip(ip)
        is_avail = circuit_breaker.is_available(ip)
        color = "green" if record.state == IpHealthState.HEALTHY else "red" if record.state == IpHealthState.BURNED else "yellow"
        cb_table.add_row(
            ip,
            f"[{color}]{record.state.value.upper()}[/{color}]",
            str(record.consecutive_failures),
            "[green]YES[/green]" if is_avail else "[red]NO[/red]",
        )
    console.print(cb_table)

    # 6. Demonstrate Wave Sniping (Cart Release)
    console.print("\n[bold cyan]>[/bold cyan] [bold white]Simulating Wave Sniping for Returned Carts...[/bold white]")
    console.print("[dim]Waiting 1.6s for unpurchased cart holds to expire on server...[/dim]")
    await asyncio.sleep(1.6)

    sniper_executor = executors[0]  # Use first healthy executor
    sniper_res = await sniper_executor.monitor_cart_releases(
        poll_interval_sec=0.10,
        max_duration_sec=2.0,
        wave_poll_interval_sec=0.05,
    )

    if sniper_res and sniper_res.success:
        console.print(
            f"[bold green]>[/bold green] [bold white]WAVE SNIPED SUCCESSFULLY ![/bold white] "
            f"Token: [cyan]{sniper_executor.active_cart.token if sniper_executor.active_cart else 'None'}[/cyan]"
        )

    # 7. Export Nanosecond Audit JSON Trace
    audit_path = os.path.abspath("reports/demo_audit_trace.json")
    shared_telemetry.export_audit_json(audit_path)
    console.print(f"[bold green]>[/bold green] Audit trace exported: [dim]{audit_path}[/dim]")

    # Shutdown all clients
    for exec_inst in executors:
        await exec_inst.shutdown()


if __name__ == "__main__":
    asyncio.run(run_simulation())
