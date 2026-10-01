#!/usr/bin/env python3
"""
Execution Algo - Unified Runner
--------------------------------
The main entry point for executing algorithm tasks without modifying Python code.
Usage:
    python run.py --config config/retail_drop.json
    python run.py --config config/retail_drop.json --url https://mon-shop.com/api --item TICKET_123
"""

import argparse
import asyncio
import os
import sys
import time
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from rich.console import Console
from rich.panel import Panel

from core.config import load_task_config
from core.engine.base import Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.rate_limiter.limiter import AdaptiveRateLimiter
from core.system import boost_process_performance, restore_process_performance
from core.telemetry.tracker import LatencyTracker
from modules.retail.checkout.state_machine import (
    CheckoutProfile,
    FastCheckoutStateMachine,
)
from modules.retail.clock.ntp_sync import HighPrecisionScheduler, NtpClient

console = Console()


# [FEATURE: UNIFIED_RUNNER_PIPELINE] Robust timestamp parsing, clean process restoration, and ticket drop support
# Raison: Prevents KeyError on dispatch_error_us, ValueError on ISO datetime, un-restored timer resolution, and unifies ticket and retail workflows.
# Attention: restore_process_performance() is guaranteed via try-finally block.
def parse_target_timestamp(raw_target: Any) -> float:
    """Parses numeric epoch or ISO 8601 datetime string into a UTC epoch timestamp."""
    if isinstance(raw_target, (int, float)):
        return float(raw_target)
    target_str = str(raw_target).strip()
    try:
        return float(target_str)
    except ValueError:
        pass
    try:
        from datetime import datetime, timezone
        if target_str.endswith("Z"):
            target_str = target_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(target_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception as parse_err:
        raise ValueError(f"Cannot parse target timestamp '{target_str}': {parse_err}")


async def execute_task(config: dict):
    boosted = boost_process_performance()
    if boosted:
        console.print("[dim][System] Priorité CPU élevée & interruption 1ms activées.[/dim]")

    try:
        task_mode = config.get("mode", "retail")
        if task_mode == "ticket" or "event_id" in config.get("target", {}):
            await execute_ticket_task(config)
            return

        task_name = config.get("task_name", "Execution Task")
        target = config["target"]
        scheduling = config.get("scheduling", {})
        rate_cfg = config.get("rate_limiting", {})
        profile_cfg = config.get("customer_profile", {})

        console.print(
            Panel(
                f"[bold cyan]Tâche :[/bold cyan] {task_name}\n"
                f"[bold cyan]Cible :[/bold cyan] {target['base_url']}\n"
                f"[bold cyan]Article / Variante :[/bold cyan] {target.get('item_id', 'N/A')}\n"
                f"[bold cyan]Mode d'Ordonnancement :[/bold cyan] {scheduling.get('mode', 'immediate')}",
                title="[bold green]Lancement de l'Algorithme d'Exécution[/bold green]",
                expand=False,
            )
        )

        # 1. Telemetry and Rate Limiter
        telemetry = LatencyTracker()
        rate_limiter = AdaptiveRateLimiter(
            base_rate=float(rate_cfg.get("requests_per_second", 5.0)),
            burst_capacity=float(rate_cfg.get("burst_capacity", 10.0)),
        )

        # 2. Pre-warmed Client
        http_client = PrewarmedHttpClient(
            base_url=target["base_url"],
            rate_limiter=rate_limiter,
            telemetry=telemetry,
        )

        # 3. Customer Profile
        profile = CheckoutProfile(
            email=profile_cfg.get("email", "client@example.com"),
            shipping_address=profile_cfg.get("shipping_address", {}),
        )

        # 4. State Machine
        fsm = FastCheckoutStateMachine(
            target_domain=target["base_url"],
            http_client=http_client,
            profile=profile,
            telemetry=telemetry,
        )

        console.print("[dim]Établissement du canal persistant et pré-chauffe TLS...[/dim]")
        await fsm.initialize()
        console.print("[green][OK] Connexion TLS active et maintenue en mémoire.[/green]")

        # 5. Scheduling (T0 or immediate)
        lead_time_ms = float(scheduling.get("lead_time_ms", 10.0))
        target_utc_raw = scheduling.get("target_time_utc")

        if target_utc_raw and scheduling.get("mode") == "scheduled":
            console.print("[dim]Synchronisation de l'horloge avec les serveurs atomiques NTP...[/dim]")
            ntp = NtpClient()
            sync_info = ntp.sync()
            console.print(f"[green][OK] Décalage d'horloge compensé : {sync_info.get('median_offset_ms', 0):+.3f} ms[/green]")

            scheduler = HighPrecisionScheduler(ntp)
            target_atomic_timestamp = parse_target_timestamp(target_utc_raw)
            console.print(f"[bold yellow]>> En attente du créneau T0 avec {lead_time_ms} ms d'avance réseau...[/bold yellow]")
            metrics = await scheduler.wait_until_atomic_timestamp(target_atomic_timestamp, latency_advance_ms=lead_time_ms)
            dispatch_err = metrics.get("dispatch_error_us", 0.0)
            console.print(f"[bold green]>> T0 ATTEINT ! Précision temporelle : {dispatch_err:.1f} µs[/bold green]")

        # 6. Execute Action
        console.print("\n[bold yellow]>> Déclenchement de l'ordre d'achat...[/bold yellow]")
        signal = Signal(
            source="runner",
            target_id=target.get("item_id", "item_default"),
            action="BUY",
            payload={
                "item_id": target.get("item_id"),
                "quantity": target.get("quantity", 1),
                "reserve_endpoint": target.get("reserve_endpoint", "/api/cart"),
                "reserve_method": target.get("reserve_method", "GET"),
                "shipping_endpoint": target.get("shipping_endpoint", "/api/checkout"),
                "shipping_method": target.get("shipping_method", "GET"),
            },
            urgency=3,
        )

        result = await fsm.execute(signal)

        if result.success:
            console.print(
                f"\n[bold green]✔ EXÉCUTION RÉUSSIE ![/bold green]\n"
                f"Statut HTTP : [bold]{result.status_code}[/bold]\n"
                f"Latence totale du pipeline : [bold]{result.latency_ms:.2f} ms[/bold]\n"
                f"Détail des étapes : {result.data.get('stages', {})}\n"
            )
        else:
            console.print(
                f"\n[bold red]✖ ÉCHEC D'EXÉCUTION : {result.error}[/bold red]\n"
                f"Statut : {result.status_code}\n"
            )

        await fsm.shutdown()
    finally:
        if boosted:
            restore_process_performance()


async def execute_ticket_task(config: dict):
    from modules.retail.tickets.ticket_engine import TicketConfig, TicketDropExecutor

    task_name = config.get("task_name", "Ticket Drop Task")
    target = config.get("target", {})
    scheduling = config.get("scheduling", {})

    console.print(
        Panel(
            f"[bold cyan]Tâche :[/bold cyan] {task_name} (Mode Billetterie)\n"
            f"[bold cyan]Cible :[/bold cyan] {target.get('base_url')}\n"
            f"[bold cyan]Événement :[/bold cyan] {target.get('event_id', target.get('item_id'))}\n"
            f"[bold cyan]Catégorie :[/bold cyan] {target.get('category_id', 'PRIMARY')}",
            title="[bold green]Lancement Billetterie T0[/bold green]",
            expand=False,
        )
    )

    drop_time_utc = None
    target_utc_raw = scheduling.get("target_time_utc")
    if target_utc_raw:
        drop_time_utc = parse_target_timestamp(target_utc_raw)

    ticket_config = TicketConfig(
        platform_name=config.get("platform_name", "billetterie_drop"),
        target_url=target.get("base_url", "https://example.com"),
        event_id=target.get("event_id", target.get("item_id", "EVENT_1")),
        category_id=target.get("category_id", "CAT_1"),
        fallback_categories=target.get("fallback_categories", []),
        quantity=int(target.get("quantity", 1)),
        drop_time_utc=drop_time_utc,
        lead_time_ms=float(scheduling.get("lead_time_ms", 10.0)),
        auto_open_browser=config.get("auto_open_browser", False),
    )

    telemetry = LatencyTracker()
    http_client = PrewarmedHttpClient(base_url=ticket_config.target_url, telemetry=telemetry)
    executor = TicketDropExecutor(config=ticket_config, http_client=http_client, telemetry=telemetry)

    console.print("[dim]Initialisation et pré-chauffe de la route billetterie...[/dim]")
    await executor.initialize()
    console.print("[green][OK] Sockets pré-chauffées et requêtes binaires pré-compilées.[/green]")

    result = await executor.execute_drop()
    if result.success:
        console.print(
            f"\n[bold green]✔ PANIER RÉSERVÉ AVEC SUCCÈS ![/bold green]\n"
            f"Statut HTTP : [bold]{result.status_code}[/bold]\n"
            f"Latence de tir : [bold]{result.latency_ms:.2f} ms[/bold]\n"
            f"Données panier : {result.data}\n"
        )
    else:
        console.print(
            f"\n[bold red]✖ ÉCHEC DE RÉSERVATION : {result.error}[/bold red]\n"
            f"Statut : {result.status_code}\n"
        )

    await executor.shutdown()


def main():
    parser = argparse.ArgumentParser(description="Execution Algo - Production Runner")
    parser.add_argument("--config", type=str, default="config/retail_drop.json", help="Path to JSON task configuration")
    parser.add_argument("--mode", type=str, choices=["retail", "ticket"], default=None, help="Execution mode override")
    parser.add_argument("--url", type=str, default=None, help="Override target base URL")
    parser.add_argument("--item", type=str, default=None, help="Override target item ID")

    args = parser.parse_args()

    config = load_task_config(args.config)

    # CLI Overrides
    if args.mode:
        config["mode"] = args.mode
    if args.url:
        config["target"]["base_url"] = args.url
    if args.item:
        config["target"]["item_id"] = args.item

    asyncio.run(execute_task(config))


if __name__ == "__main__":
    main()
