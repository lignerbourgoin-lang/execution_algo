"""
Notification Channels
---------------------
The human stays in the loop: the engine detects fast, the person validates the purchase.
Channels:
- ConsoleNotifier: log line (always on, also the audit trail).
- NtfyNotifier: push notification to a phone through ntfy (https://ntfy.sh or self-hosted).
  Tapping the notification opens the listing / sale page directly.
- BrowserNotifier: opens the page in the local default browser.
"""

import asyncio
import logging
import webbrowser
from dataclasses import dataclass
from typing import List, Optional, Protocol, Sequence

import httpx

logger = logging.getLogger("modules.retail.notify")

DEFAULT_NTFY_SERVER = "https://ntfy.sh"
NTFY_REQUEST_TIMEOUT_SEC = 5.0
NTFY_PRIORITY_DEFAULT = 3
NTFY_PRIORITY_URGENT = 5
NTFY_MIN_TOPIC_LENGTH = 16


@dataclass(frozen=True)
class Notification:
    title: str
    message: str
    url: Optional[str] = None
    is_urgent: bool = False


class Notifier(Protocol):
    name: str

    async def send(self, notification: Notification) -> None: ...


class ConsoleNotifier:
    name = "console"

    async def send(self, notification: Notification) -> None:
        logger.info("[ALERT] %s | %s | %s", notification.title, notification.message, notification.url or "-")
        print(f"[ALERT] {notification.title}\n        {notification.message}\n        {notification.url or ''}", flush=True)


class NtfyNotifier:
    """
    Publishes through the ntfy JSON API (avoids non-ASCII header encoding issues).
    The topic name acts as a password on the public server: keep it long and random,
    and keep it out of git (config/*.json is ignored).
    """

    name = "ntfy"

    def __init__(self, topic: str, server_url: str = DEFAULT_NTFY_SERVER, transport: Optional[httpx.AsyncBaseTransport] = None):
        if len(topic) < NTFY_MIN_TOPIC_LENGTH:
            raise ValueError(f"ntfy topic must be at least {NTFY_MIN_TOPIC_LENGTH} characters (anyone knowing it reads your alerts)")
        self.topic = topic
        self.server_url = server_url.rstrip("/")
        self.client = httpx.AsyncClient(timeout=NTFY_REQUEST_TIMEOUT_SEC, transport=transport)

    async def send(self, notification: Notification) -> None:
        publish_payload = {
            "topic": self.topic,
            "title": notification.title,
            "message": notification.message,
            "priority": NTFY_PRIORITY_URGENT if notification.is_urgent else NTFY_PRIORITY_DEFAULT,
        }
        if notification.url:
            publish_payload["click"] = notification.url
        response = await self.client.post(self.server_url, json=publish_payload)
        response.raise_for_status()

    async def close(self) -> None:
        await self.client.aclose()


class BrowserNotifier:
    name = "browser"

    async def send(self, notification: Notification) -> None:
        if notification.url:
            # webbrowser.open can block on some platforms: keep it off the event loop.
            await asyncio.to_thread(webbrowser.open_new_tab, notification.url)


async def broadcast(notifiers: Sequence[Notifier], notification: Notification) -> List[str]:
    """
    Sends to all channels concurrently. One failing channel never blocks the others.
    Returns the names of the channels that failed (each failure is logged).
    """
    results = await asyncio.gather(*(n.send(notification) for n in notifiers), return_exceptions=True)
    failed_channels = []
    for notifier, result in zip(notifiers, results):
        if isinstance(result, BaseException):
            failed_channels.append(notifier.name)
            logger.error("Notifier %s failed: %r", notifier.name, result)
    return failed_channels
