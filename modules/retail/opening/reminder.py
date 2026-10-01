"""
Sale Opening Reminder
---------------------
Most large ticketing platforms place every visitor who arrives BEFORE the opening time in a
waiting room and randomize their order at opening. Arriving 5 ms early buys nothing;
arriving several minutes early, logged in, with payment ready, is what counts.

This module therefore does not race the opening. At NTP-corrected times it:
- sends reminders (e.g. T-15 min, T-3 min, T0),
- opens the sale page in your real browser at a chosen reminder, so you enter the waiting room early.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Sequence

from modules.retail.clock.ntp_sync import HighPrecisionScheduler
from modules.retail.notify.notifiers import BrowserNotifier, Notification, Notifier, broadcast

logger = logging.getLogger("modules.retail.opening")

SECONDS_PER_MINUTE = 60


@dataclass(frozen=True)
class OpeningReminderConfig:
    event_name: str
    sale_url: str
    opening_time_utc: datetime  # must be timezone-aware
    reminder_minutes_before: Sequence[float] = (15.0, 3.0, 0.0)
    open_browser_minutes_before: Optional[float] = 3.0

    def __post_init__(self):
        if self.opening_time_utc.tzinfo is None:
            raise ValueError("opening_time_utc must be timezone-aware (e.g. 2026-10-15T10:00:00+02:00)")
        if any(minutes < 0 for minutes in self.reminder_minutes_before):
            raise ValueError("reminder_minutes_before values must be >= 0")
        if (
            self.open_browser_minutes_before is not None
            and self.open_browser_minutes_before not in self.reminder_minutes_before
        ):
            # Otherwise the browser would silently never open.
            raise ValueError("open_browser_minutes_before must be one of reminder_minutes_before")


def _format_lead(minutes_before: float) -> str:
    if minutes_before == 0:
        return "C'est l'heure d'ouverture"
    return f"Ouverture dans {minutes_before:g} min"


class OpeningReminder:
    def __init__(
        self,
        config: OpeningReminderConfig,
        notifiers: Sequence[Notifier],
        scheduler: Optional[HighPrecisionScheduler] = None,
    ):
        if not notifiers:
            raise ValueError("At least one notifier is required")
        self.config = config
        self.notifiers = list(notifiers)
        self.scheduler = scheduler or HighPrecisionScheduler()
        self.browser = BrowserNotifier()
        self.sent_reminders: List[float] = []

    def pending_reminders(self, now_utc: datetime) -> List[float]:
        """Reminder offsets (minutes) still in the future, in chronological order. Past ones are logged."""
        opening_timestamp = self.config.opening_time_utc.timestamp()
        pending = []
        for minutes_before in sorted(set(self.config.reminder_minutes_before), reverse=True):
            fire_timestamp = opening_timestamp - minutes_before * SECONDS_PER_MINUTE
            if fire_timestamp < now_utc.timestamp():
                logger.warning("Reminder T-%g min already passed, skipped", minutes_before)
                continue
            pending.append(minutes_before)
        return pending

    async def run(self) -> None:
        opening_timestamp = self.config.opening_time_utc.timestamp()
        browser_offset = self.config.open_browser_minutes_before

        for minutes_before in self.pending_reminders(datetime.now(timezone.utc)):
            metrics = await self.scheduler.wait_until_atomic_timestamp(
                opening_timestamp - minutes_before * SECONDS_PER_MINUTE
            )
            notification = Notification(
                title=f"{self.config.event_name} : {_format_lead(minutes_before)}",
                message="Connecte-toi, verifie ton moyen de paiement, entre dans la file d'attente.",
                url=self.config.sale_url,
                is_urgent=minutes_before <= (browser_offset or 0),
            )
            await broadcast(self.notifiers, notification)
            if browser_offset is not None and minutes_before == browser_offset:
                await broadcast([self.browser], notification)
            self.sent_reminders.append(minutes_before)
            logger.info(
                "Reminder T-%g min sent (clock uncertainty +/- %.1f ms)",
                minutes_before,
                metrics["clock_uncertainty_ms"],
            )
