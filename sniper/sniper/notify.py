from __future__ import annotations

from typing import Protocol

import httpx


class Notify(Protocol):
    async def urgent(self, message: str) -> None: ...


class PrintNotify:
    async def urgent(self, message: str) -> None:
        print(message, flush=True)


class WebhookNotify:
    def __init__(self, url: str) -> None:
        self.url = url
        self.fallback = PrintNotify()

    async def urgent(self, message: str) -> None:
        await self.fallback.urgent(message)
        if not self.url:
            return
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(self.url, content=message.encode("utf-8"))
        except httpx.HTTPError:
            pass


def make_notify(family: dict) -> Notify:
    url = ((family.get("notify") or {}).get("webhook") or "").strip()
    return WebhookNotify(url) if url else PrintNotify()
