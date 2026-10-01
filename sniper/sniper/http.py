from __future__ import annotations

from typing import Any

import httpx


class PersistentClient:
    def __init__(self, headers: dict[str, str] | None = None, cookies: dict[str, str] | None = None) -> None:
        self.client = httpx.AsyncClient(
            http2=True,
            headers=headers or {},
            cookies=cookies or {},
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20, keepalive_expiry=300.0),
            timeout=httpx.Timeout(10.0, connect=2.0),
            follow_redirects=True,
        )

    async def warmup(self, url: str | None) -> None:
        if not url:
            return
        try:
            await self.client.get(url)
        except httpx.HTTPError:
            pass

    async def get(self, url: str, **kw: Any) -> httpx.Response:
        return await self.client.get(url, **kw)

    async def post(self, url: str, **kw: Any) -> httpx.Response:
        return await self.client.post(url, **kw)

    async def aclose(self) -> None:
        await self.client.aclose()
