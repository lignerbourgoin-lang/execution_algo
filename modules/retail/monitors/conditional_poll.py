"""
Conditional HTTP Polling Monitor (ETag / If-None-Match / 304 Not Modified)
-------------------------------------------------------------------------
Monitors resource endpoints at high frequency with minimum bandwidth and quota usage:
- Leverages ETag and Last-Modified caching headers
- Receives 304 Not Modified (0-byte payload) when no state change occurs
- Immediately emits an event when status transitions to 200 OK
"""

import asyncio
import logging
import time
from typing import Any, Callable, Coroutine, Dict, Optional
import httpx

from core.engine.base import Signal

logger = logging.getLogger("modules.retail.monitors.conditional")


class ConditionalPoller:
    """
    High-frequency conditional polling monitor using standard HTTP cache validation.
    """

    def __init__(
        self,
        url: str,
        poll_interval_sec: float = 1.0,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.url = url
        self.poll_interval_sec = poll_interval_sec
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
        }

        self.last_etag: Optional[str] = None
        self.last_modified: Optional[str] = None
        self._is_running = False
        self._task: Optional[asyncio.Task] = None
        self._on_change_callbacks: list[Callable[[Dict[str, Any], int], Coroutine[Any, Any, None]]] = []

        self.checks_count = 0
        self.not_modified_count = 0
        self.changes_detected_count = 0

    def on_change(self, callback: Callable[[Dict[str, Any], int], Coroutine[Any, Any, None]]):
        """Register async callback triggered when resource content changes."""
        self._on_change_callbacks.append(callback)

    async def start(self):
        self._is_running = True
        self._task = asyncio.create_task(self._poll_loop())

    async def _poll_loop(self):
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=5)
        async with httpx.AsyncClient(limits=limits, headers=self.headers, timeout=5.0) as client:
            while self._is_running:
                req_headers = dict(self.headers)
                if self.last_etag:
                    req_headers["If-None-Match"] = self.last_etag
                if self.last_modified:
                    req_headers["If-Modified-Since"] = self.last_modified

                t0 = time.perf_counter_ns()
                try:
                    res = await client.get(self.url, headers=req_headers)
                    t_recv = time.perf_counter_ns()
                    self.checks_count += 1

                    if res.status_code == 304:
                        # Resource unchanged - 0 byte download
                        self.not_modified_count += 1

                    elif res.status_code == 200:
                        # Resource modified or first fetch
                        new_etag = res.headers.get("ETag") or res.headers.get("etag")
                        new_modified = res.headers.get("Last-Modified") or res.headers.get("last-modified")

                        is_real_change = (self.last_etag is not None or self.last_modified is not None)
                        self.last_etag = new_etag
                        self.last_modified = new_modified

                        if is_real_change:
                            self.changes_detected_count += 1
                            try:
                                data = res.json()
                            except Exception:
                                data = {"text": res.text}

                            for cb in self._on_change_callbacks:
                                asyncio.create_task(cb(data, t_recv))

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.debug(f"Conditional poll error: {e}")

                await asyncio.sleep(self.poll_interval_sec)

    async def stop(self):
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
