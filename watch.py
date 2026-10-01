#!/usr/bin/env python3
"""
Ticketing Watch Runner
----------------------
Two human-in-the-loop tools for ticketing:
    python watch.py resale  --config config/resale_watch.json
    python watch.py opening --config config/sale_opening.json

resale  : alerts (phone push + browser) when a matching ticket appears on an official resale feed.
opening : NTP-corrected reminders before a sale opens, and opens the page early in your browser.
"""

import argparse
import asyncio
import logging
import os
import sys
from decimal import Decimal
from typing import Any, Dict, List

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from core.config import ConfigError, load_json_config, parse_aware_datetime
from modules.retail.clock.ntp_sync import ClockSyncError
from modules.retail.notify.notifiers import BrowserNotifier, ConsoleNotifier, Notifier, NtfyNotifier
from modules.retail.opening.reminder import OpeningReminder, OpeningReminderConfig
from modules.retail.resale.watcher import ListingFieldPaths, ListingFilters, ResaleWatchConfig, ResaleWatcher

logger = logging.getLogger("watch")


def build_notifiers(notify_config: Dict[str, Any]) -> List[Notifier]:
    notifiers: List[Notifier] = [ConsoleNotifier()]
    ntfy_topic = notify_config.get("ntfy_topic")
    if ntfy_topic:
        notifiers.append(NtfyNotifier(topic=ntfy_topic, server_url=notify_config.get("ntfy_server", "https://ntfy.sh")))
    if notify_config.get("open_browser", False):
        notifiers.append(BrowserNotifier())
    return notifiers


def build_resale_config(raw_config: Dict[str, Any]) -> ResaleWatchConfig:
    source = raw_config.get("source", {})
    if not source.get("url"):
        raise ConfigError("source.url is required")
    raw_fields = source.get("fields", {})
    raw_filters = raw_config.get("filters", {})
    max_price = raw_filters.get("max_price")
    return ResaleWatchConfig(
        url=source["url"],
        items_path=source.get("items_path", ""),
        fields=ListingFieldPaths(
            listing_id=raw_fields.get("id", "id"),
            price=raw_fields.get("price", "price"),
            title=raw_fields.get("title", "title"),
            url=raw_fields.get("url", "url"),
            quantity=raw_fields.get("quantity", "quantity"),
        ),
        filters=ListingFilters(
            max_price=Decimal(str(max_price)) if max_price is not None else None,
            min_quantity=int(raw_filters.get("min_quantity", 1)),
            title_keywords=tuple(raw_filters.get("title_keywords", ())),
            excluded_keywords=tuple(raw_filters.get("excluded_keywords", ())),
        ),
        poll_interval_sec=float(source.get("poll_interval_sec", 5.0)),
        headers=source.get("headers", {}),
        alert_existing_on_start=bool(raw_config.get("alert_existing_on_start", True)),
    )


async def run_resale(raw_config: Dict[str, Any]) -> None:
    watcher = ResaleWatcher(build_resale_config(raw_config), build_notifiers(raw_config.get("notify", {})))
    await watcher.start()
    logger.info("Watching %s every %.1fs (Ctrl+C to stop)", watcher.config.url, watcher.config.poll_interval_sec)
    try:
        await asyncio.Event().wait()
    finally:
        await watcher.stop()


async def run_opening(raw_config: Dict[str, Any]) -> None:
    reminder_config = OpeningReminderConfig(
        event_name=raw_config.get("event_name", "Ouverture des ventes"),
        sale_url=raw_config["sale_url"],
        opening_time_utc=parse_aware_datetime(raw_config.get("opening_time"), "opening_time"),
        reminder_minutes_before=tuple(raw_config.get("reminder_minutes_before", (15.0, 3.0, 0.0))),
        open_browser_minutes_before=raw_config.get("open_browser_minutes_before", 3.0),
    )
    reminder = OpeningReminder(reminder_config, build_notifiers(raw_config.get("notify", {})))
    await reminder.run()
    logger.info("All reminders sent: %s", reminder.sent_reminders)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Ticketing watch tools (human validates the purchase)")
    parser.add_argument("mode", choices=("resale", "opening"))
    parser.add_argument("--config", required=True, help="Path to JSON configuration")
    args = parser.parse_args()

    try:
        raw_config = load_json_config(args.config)
        runner = run_resale if args.mode == "resale" else run_opening
        asyncio.run(runner(raw_config))
    except (ConfigError, FileNotFoundError, KeyError, ValueError) as error:
        logger.error("Invalid configuration: %s", error)
        return 2
    except ClockSyncError as error:
        logger.error("Clock synchronisation failed, aborting: %s", error)
        return 3
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    return 0


if __name__ == "__main__":
    sys.exit(main())
