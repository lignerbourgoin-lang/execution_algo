"""
Demonstration: Multi-IP Lottery Queue Orchestrator (20 IPs)
------------------------------------------------------------
Simulates 20 concurrent connections entering a virtual waiting room.
Ranks all assigned queue numbers, retains golden tickets and fallback winners,
and cleanly prunes non-viable connections.
"""

import asyncio
import os
import random
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from modules.retail.tickets.lottery_selector import (
    LotteryQueueSelector,
    LotteryTicket,
    MultiIpLotteryOrchestrator,
)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

console = Console()

# 20 Simulated IPs / Dedicated Proxies
SAMPLE_20_IPS = [f"192.168.10.{i}" for i in range(1, 21)]


async def simulate_waiting_room_entry(ip_address: str) -> int:
    """
    Simulates sending the initial request to the waiting room queue from a specific IP.
    Returns a random queue rank based on a 50,000-person waiting room.
    """
    # Realistic network RTT jitter between 20ms and 75ms
    await asyncio.sleep(random.uniform(0.02, 0.075))

    # Queue shuffle: positions from 1 to 50,000
    # Include an occasional top ticket for demonstration
    if random.random() < 0.25:
        return random.randint(15, 600)  # Near front
    return random.randint(601, 50_000)


async def main():
    console.print(
        Panel.fit(
            "[bold cyan]EXECUTION ENGINE[/bold cyan] : [bold yellow]Multi-IP Lottery Queue (20 IPs)[/bold yellow]\n"
            "[dim]Strategy: Parallel Entry at T0 -> Rank Sorting -> Adaptive Retention[/dim]",
            border_style="cyan",
        )
    )

    selector = LotteryQueueSelector(lower_is_better=True)
    orchestrator = MultiIpLotteryOrchestrator(
        selector=selector,
        concurrency_limit=20,  # Firing all 20 simultaneously
        timeout_sec=5.0,
    )

    console.print(f"[bold green]>>[/bold green] Firing simultaneous queue requests across {len(SAMPLE_20_IPS)} IPs...")
    start_time = time.perf_counter()

    # Query all 20 IPs in parallel
    await orchestrator.survey_pool(
        ips_or_pool=SAMPLE_20_IPS,
        query_func=simulate_waiting_room_entry,
    )

    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    console.print(f"[bold green]OK[/bold green] All 20 numbers received in [bold yellow]{elapsed_ms:.1f} ms[/bold yellow].\n")

    # Apply adaptive selection:
    # - Golden tickets: any position <= 500
    # - Fallback: keep at least the top 2 best tickets even if none are <= 500
    # - Ceiling: max 5 sessions kept
    kept, discarded = selector.select_adaptive(
        golden_threshold=500,
        min_keep=2,
        max_keep=5,
    )

    # Format Rich Table
    table = Table(title="Lottery Queue Results (Sorted by Priority)", border_style="dim")
    table.add_column("Rank", justify="center", style="bold")
    table.add_column("IP Address", justify="center")
    table.add_column("Queue Position", justify="right")
    table.add_column("Status", justify="center")
    table.add_column("Decision", justify="left")

    all_sorted = selector.get_best_tickets()
    for index, ticket in enumerate(all_sorted, start=1):
        pos = ticket.queue_number
        if ticket.status == "selected":
            if pos <= 500:
                status_str = "[bold green]GOLDEN[/bold green]"
                decision_str = "[green]Retained (Exceptional rank <= 500)[/green]"
            else:
                status_str = "[bold yellow]SELECTED[/bold yellow]"
                decision_str = "[yellow]Retained (Fallback security)[/yellow]"
            pos_str = f"[bold green]#{pos:,}[/bold green]"
        else:
            status_str = "[bold red]PRUNED[/bold red]"
            decision_str = "[dim red]Socket closed (Rank too distant)[/dim red]"
            pos_str = f"[dim]#{pos:,}[/dim]"

        table.add_row(str(index), ticket.ip_address, pos_str, status_str, decision_str)

    console.print(table)

    best = selector.best_ticket()
    console.print(
        Panel.fit(
            f"[bold green]PRIMARY TARGET :[/bold green] IP [bold cyan]{best.ip_address}[/bold cyan] "
            f"holds queue rank [bold yellow]#{best.queue_number:,}[/bold yellow]!\n"
            f"[dim]Active connections preserved: {len(kept)} | Sockets terminated: {len(discarded)}[/dim]",
            border_style="green",
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
