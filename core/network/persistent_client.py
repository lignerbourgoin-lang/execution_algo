"""
Pre-Warmed Persistent HTTP Connection Pool
------------------------------------------
Keeps TLS sockets permanently hot and connected to target hosts.
Eliminates DNS, TCP 3-way handshake, and TLS negotiation from the execution path.
HTTP/2 is enabled by default: one multiplexed connection, no head-of-line blocking
between the heartbeat and the critical request.
"""

import asyncio
import importlib.util
import logging
import time
from typing import Any, Dict, Optional

import httpx

from core.rate_limiter.limiter import AdaptiveRateLimiter
from core.telemetry.tracker import LatencyTracker

logger = logging.getLogger("core.network.http")

DEFAULT_USER_AGENT = "ExecutionEngine/2.0"
DEFAULT_REQUEST_TIMEOUT_SEC = 10.0
PREWARM_TIMEOUT_SEC = 5.0
HEARTBEAT_TIMEOUT_SEC = 3.0
KEEPALIVE_EXPIRY_SEC = 60.0
MAX_KEEPALIVE_CONNECTIONS = 10
MAX_CONNECTIONS = 20
NS_PER_MS = 1_000_000.0


class PrewarmedHttpClient:
    """
    Manages persistent HTTP sessions with periodic low-frequency heartbeats
    to ensure the TLS socket remains alive and ready for low-latency burst execution.
    """

    def __init__(
        self,
        base_url: str,
        heartbeat_interval_sec: float = 20.0,
        rate_limiter: Optional[AdaptiveRateLimiter] = None,
        telemetry: Optional[LatencyTracker] = None,
        headers: Optional[Dict[str, str]] = None,
        http2: bool = True,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.heartbeat_interval_sec = heartbeat_interval_sec
        self.rate_limiter = rate_limiter or AdaptiveRateLimiter(base_rate=10.0, burst_capacity=20.0)
        self.telemetry = telemetry or LatencyTracker()

        # [FEATURE: HTTP2_FAIL_CLOSED] HTTP/2 is requested explicitly and refused loudly if unavailable.
        # Raison: the README promised HTTP/2 but the client silently spoke HTTP/1.1.
        #         Silently degrading would hide a latency regression.
        # Attention: the "Connection: keep-alive" header was removed: it is forbidden in HTTP/2
        #            (RFC 9113 section 8.2.2) and httpx keeps connections alive by default.
        if http2 and importlib.util.find_spec("h2") is None:
            raise RuntimeError("http2=True requires the 'h2' package (pip install -r requirements.txt)")

        default_headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json, text/plain, */*",
        }
        if headers:
            default_headers.update(headers)

        limits = httpx.Limits(
            max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
            max_connections=MAX_CONNECTIONS,
            keepalive_expiry=KEEPALIVE_EXPIRY_SEC,
        )
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            http2=http2,
            limits=limits,
            headers=default_headers,
            timeout=DEFAULT_REQUEST_TIMEOUT_SEC,
            verify=True,
            transport=transport,
        )

        self._heartbeat_task: Optional[asyncio.Task] = None
        self._is_running = False
        self.is_warmed_up = False
        self.negotiated_http_version: Optional[str] = None

    async def start(self):
        """Initializes and pre-warms connection, then starts heartbeat loop."""
        self._is_running = True
        await self.prewarm()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def prewarm(self) -> bool:
        """Sends an initial probe to complete DNS, TCP and TLS handshakes."""
        try:
            response = await self.client.head("/", timeout=PREWARM_TIMEOUT_SEC)
        except httpx.HTTPError as head_error:
            logger.info("HEAD prewarm failed (%s), retrying with streamed GET", head_error)
            try:
                async with self.client.stream("GET", "/", timeout=PREWARM_TIMEOUT_SEC) as response:
                    pass
            except httpx.HTTPError as get_error:
                logger.warning("Prewarm failed for %s: %s", self.base_url, get_error)
                self.is_warmed_up = False
                return False

        self.negotiated_http_version = response.http_version
        self.is_warmed_up = True
        logger.info("Prewarmed %s over %s", self.base_url, self.negotiated_http_version)
        return True

    async def _heartbeat_loop(self):
        """Background loop to keep sockets alive in server/NAT state tables."""
        while self._is_running:
            await asyncio.sleep(self.heartbeat_interval_sec)
            try:
                await self.client.head("/", timeout=HEARTBEAT_TIMEOUT_SEC)
                self.is_warmed_up = True
            except httpx.HTTPError as error:
                # The next request will reconnect, but the operator must know the socket is cold.
                self.is_warmed_up = False
                logger.warning("Heartbeat to %s failed: %s", self.base_url, error)

    async def execute_fast(
        self,
        method: str,
        endpoint: str,
        action_id: str,
        json_data: Optional[Dict[str, Any]] = None,
        content: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executes a priority action on the pre-warmed connection with microsecond tracking.
        Supports both json_data and pre-serialized raw bytes.
        Never raises on network errors: returns status_code 0 with an "error" field.
        """
        trace = self.telemetry.start_trace(action_id=action_id, target=f"{self.base_url}{endpoint}")

        await self.rate_limiter.wait_for_slot()
        trace.mark_stage("rate_limiter_acquired")

        request_headers = dict(headers) if headers else {}
        if idempotency_key:
            # Honoured by servers implementing the IETF Idempotency-Key draft; ignored elsewhere.
            request_headers["Idempotency-Key"] = idempotency_key

        request_kwargs: Dict[str, Any] = {}
        if content is not None:
            request_kwargs["content"] = content
            if not any(name.lower() == "content-type" for name in request_headers):
                request_headers["Content-Type"] = "application/json"
        elif json_data is not None:
            request_kwargs["json"] = json_data

        dispatch_ns = time.perf_counter_ns()
        try:
            response = await self.client.request(
                method=method,
                url=endpoint,
                headers=request_headers or None,
                **request_kwargs,
            )
        except httpx.HTTPError as error:
            logger.warning("%s %s%s failed: %r", method, self.base_url, endpoint, error)
            trace.complete(success=False, error=repr(error))
            return {
                "status_code": 0,
                "error": repr(error),
                "latency_breakdown": trace.get_breakdown(),
            }

        received_ns = time.perf_counter_ns()
        trace.mark_stage("response_received")
        self.rate_limiter.on_response(response.status_code, dict(response.headers))

        content_type = response.headers.get("content-type", "")
        body: Any = response.text
        if "application/json" in content_type:
            try:
                body = response.json()
            except ValueError:
                logger.warning("Invalid JSON body from %s%s despite content-type", self.base_url, endpoint)

        trace.complete(success=(response.status_code < 400))
        return {
            "status_code": response.status_code,
            "body": body,
            "headers": dict(response.headers),
            "http_version": response.http_version,
            "latency_breakdown": trace.get_breakdown(),
            "network_ms": round((received_ns - dispatch_ns) / NS_PER_MS, 3),
        }

    async def close(self):
        """Clean shutdown of heartbeat and HTTP client session."""
        self._is_running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        await self.client.aclose()


# Alias for intuitive naming
PersistentHttpClient = PrewarmedHttpClient

