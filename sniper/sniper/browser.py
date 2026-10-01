from __future__ import annotations

from typing import Any, Protocol


class BrowserPort(Protocol):
    async def warmup(self, url: str) -> None: ...
    async def goto(self, url: str) -> None: ...
    async def close(self) -> None: ...


class PlaywrightBrowser:
    def __init__(self, cookies: dict[str, str] | None = None) -> None:
        self.cookies = cookies or {}
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    async def start(self) -> None:
        try:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
            self._context = await self._browser.new_context()
            self._page = await self._context.new_page()
        except ImportError:
            pass

    async def warmup(self, url: str) -> None:
        if self._page and url:
            try:
                await self._page.goto(url, wait_until="domcontentloaded", timeout=10000)
            except Exception:
                pass

    async def goto(self, url: str) -> None:
        if self._page and url:
            await self._page.goto(url)

    async def close(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
