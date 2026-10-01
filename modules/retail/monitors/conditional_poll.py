"""
Conditional HTTP Polling Monitor (ETag / If-None-Match / 304 Not Modified)
-------------------------------------------------------------------------
Monitors a resource endpoint with minimum bandwidth and quota usage:
- Leverages ETag and Last-Modified caching headers (304 Not Modified = empty payload)
- Falls back to a content hash when the server sends neither header
- Backs off on 429 / 503 (Retry-After honoured) through the adaptive rate limiter
"""

import asyncio
import hashlib
import logging
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional

import httpx

from core.rate_limiter.limiter import AdaptiveRateLimiter
from core.tasks import BackgroundTaskSet

logger = logging.getLogger("modules.retail.monitors.conditional")

HTTP_OK = 200
HTTP_NOT_MODIFIED = 304
# Floor on the polling period: below it, the poller is indistinguishable from abuse
# and gets the account / IP rate-limited, which costs far more latency than it saves.
MIN_POLL_INTERVAL_SEC = 1.0
POLL_REQUEST_TIMEOUT_SEC = 5.0
DEFAULT_USER_AGENT = "ExecutionEngine/2.0 (resource monitor)"

ChangeCallback = Callable[[Dict[str, Any], int], Coroutine[Any, Any, None]]


class ConditionalPoller:
    """
    Conditional polling monitor using standard HTTP cache validation.
    Callbacks receive (parsed_body, receive_time_perf_ns).
    """

    def __init__(
        self,
        url: str,
        poll_interval_sec: float = 2.0,
        headers: Optional[Dict[str, str]] = None,
        emit_initial: bool = False,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        http2: bool = True,
    ):
        if poll_interval_sec < MIN_POLL_INTERVAL_SEC:
            raise ValueError(f"poll_interval_sec must be >= {MIN_POLL_INTERVAL_SEC}s (got {poll_interval_sec})")
        self.url = url
        self.poll_interval_sec = poll_interval_sec
        self.headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json, text/plain, */*"}
        if headers:
            self.headers.update(headers)
        self.emit_initial = emit_initial
        self._transport = transport
        self._http2 = http2
        # One request per poll period on average, small burst for retries.
        self.rate_limiter = AdaptiveRateLimiter(
            base_rate=1.0 / poll_interval_sec,
            burst_capacity=2.0,
            min_rate=1.0 / (poll_interval_sec * 16),
        )

        self.last_etag: Optional[str] = None
        self.last_modified: Optional[str] = None
        self.last_content_hash: Optional[str] = None
        self._is_running = False
        self._task: Optional[asyncio.Task] = None
        self._on_change_callbacks: List[ChangeCallback] = []
        self._callback_tasks = BackgroundTaskSet(owner_name=f"poller:{url}")

        self.checks_count = 0
        self.not_modified_count = 0
        self.changes_detected_count = 0
        self.error_count = 0

    def on_change(self, callback: ChangeCallback):
        """Register async callback triggered when resource content changes."""
        self._on_change_callbacks.append(callback)

    async def start(self):
        self._is_running = True
        self._task = asyncio.create_task(self._poll_loop())

    def _build_request_headers(self) -> Dict[str, str]:
        request_headers: Dict[str, str] = {}
        if self.last_etag:
            request_headers["If-None-Match"] = self.last_etag
        if self.last_modified:
            request_headers["If-Modified-Since"] = self.last_modified
        return request_headers

    def _process_ok_response(self, response: httpx.Response, received_ns: int) -> None:
        # [FEATURE: POLLER_CONTENT_HASH] Change detection no longer depends on ETag / Last-Modified.
        # Raison: without those headers, the old poller considered every 200 as the
        #         "first fetch" forever and NEVER fired: a silent no-op monitor.
        # Attention: the hash covers the raw body; volatile fields (timestamps) in the body
        #            will trigger on every poll, so filter at the consumer (see resale watcher).
        content_hash = hashlib.blake2b(response.content, digest_size=16).hexdigest()
        is_first_fetch = self.last_content_hash is None
        is_changed = content_hash != self.last_content_hash

        self.last_etag = response.headers.get("etag")
        self.last_modified = response.headers.get("last-modified")
        self.last_content_hash = content_hash

        if not is_changed or (is_first_fetch and not self.emit_initial):
            return

        self.changes_detected_count += 1
        try:
            data = response.json()
        except ValueError:
            data = {"text": response.text}
        for callback in self._on_change_callbacks:
            self._callback_tasks.spawn(callback(data, received_ns))

    async def poll_once(self, client: httpx.AsyncClient) -> Optional[int]:
        """Performs one conditional request. Returns the HTTP status, or None on network error."""
        await self.rate_limiter.wait_for_slot()
        try:
            response = await client.get(self.url, headers=self._build_request_headers())
        except httpx.HTTPError as error:
            self.error_count += 1
            logger.warning("Poll %s failed: %r", self.url, error)
            return None

        received_ns = time.perf_counter_ns()
        self.checks_count += 1
        self.rate_limiter.on_response(response.status_code, dict(response.headers))

        if response.status_code == HTTP_NOT_MODIFIED:
            self.not_modified_count += 1
        elif response.status_code == HTTP_OK:
            self._process_ok_response(response, received_ns)
        else:
            self.error_count += 1
            logger.warning("Poll %s returned HTTP %d", self.url, response.status_code)
        return response.status_code

    async def _poll_loop(self):
        limits = httpx.Limits(max_keepalive_connections=2, max_connections=2)
        async with httpx.AsyncClient(
            limits=limits,
            headers=self.headers,
            timeout=POLL_REQUEST_TIMEOUT_SEC,
            http2=self._http2,
            transport=self._transport,
        ) as client:
            while self._is_running:
                await self.poll_once(client)
                await asyncio.sleep(self.poll_interval_sec)

    async def stop(self):
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._callback_tasks.cancel_all()
