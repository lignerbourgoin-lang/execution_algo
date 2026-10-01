"""
Pre-Warmed Persistent HTTP Connection Pool
------------------------------------------
Keeps TLS sockets permanently hot and connected to target hosts.
Eliminates DNS, TCP 3-way handshake, and TLS negotiation from the execution path.
"""

import asyncio
import time
from typing import Any, Dict, List, Optional
import httpx

from core.rate_limiter.limiter import AdaptiveRateLimiter
from core.telemetry.tracker import ExecutionTrace, LatencyTracker


class PrewarmedHttpClient:
    """
    Manages persistent HTTP sessions with periodic low-frequency heartbeats
    to ensure the TLS socket remains alive and ready for zero-latency burst execution.
    """

    def __init__(
        self,
        base_url: str,
        heartbeat_interval_sec: float = 20.0,
        rate_limiter: Optional[AdaptiveRateLimiter] = None,
        telemetry: Optional[LatencyTracker] = None,
        headers: Optional[Dict[str, str]] = None,
        client: Optional[httpx.AsyncClient] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.heartbeat_interval_sec = heartbeat_interval_sec
        self.rate_limiter = rate_limiter or AdaptiveRateLimiter(base_rate=10.0, burst_capacity=20.0)
        self.telemetry = telemetry or LatencyTracker()

        default_headers = {
            "User-Agent": "ExecutionEngine/2.0 (HighLatencyOptimized)",
            "Accept": "application/json, text/plain, */*",
            "Connection": "keep-alive",
        }
        if headers:
            default_headers.update(headers)

        if client is not None:
            self.client = client
        else:
            # Check HTTP/2 support (h2 package)
            try:
                import h2  # noqa: F401
                has_h2 = True
            except ImportError:
                has_h2 = False

            limits = httpx.Limits(max_keepalive_connections=10, max_connections=20, keepalive_expiry=60.0)
            self.client = httpx.AsyncClient(
                base_url=self.base_url,
                http2=has_h2,
                limits=limits,
                headers=default_headers,
                timeout=10.0,
                verify=True,
            )

        self._heartbeat_task: Optional[asyncio.Task] = None
        self._is_running = False
        self.is_warmed_up = False

    async def start(self):
        """Initializes and pre-warms connection, then starts heartbeat loop."""
        self._is_running = True
        await self.prewarm()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def prewarm(self) -> bool:
        """Sends an initial probe to complete DNS, TCP and TLS handshakes."""
        try:
            res = await self.client.head("/", timeout=5.0)
            self.is_warmed_up = True
            return True
        except Exception:
            try:
                # Some servers disallow HEAD, try GET with stream
                async with self.client.stream("GET", "/", timeout=5.0) as _:
                    self.is_warmed_up = True
                    return True
            except Exception:
                return False

    async def _heartbeat_loop(self):
        """Background loop to keep sockets alive in server/NAT state tables."""
        while self._is_running:
            await asyncio.sleep(self.heartbeat_interval_sec)
            try:
                await self.client.head("/", timeout=3.0)
            except Exception:
                # If connection dropped, next request will reconnect
                pass

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
        Supports standard Idempotency-Key header to prevent duplicate execution upon retries.
        """
        trace = self.telemetry.start_trace(action_id=action_id, target=f"{self.base_url}{endpoint}")

        # 1. Rate limiter check (wait if penalized)
        wait_ms = await self.rate_limiter.wait_for_slot()
        trace.mark_stage("rate_limiter_acquired")

        # 2. Dispatch request over warm socket
        t_dispatch = time.perf_counter_ns()
        try:
            req_kwargs = {}
            req_headers = dict(headers) if headers else {}

            if idempotency_key:
                req_headers["Idempotency-Key"] = idempotency_key

            if content is not None:
                req_kwargs["content"] = content
                if "Content-Type" not in req_headers and "content-type" not in req_headers:
                    req_headers["Content-Type"] = "application/json"
            elif json_data is not None:
                req_kwargs["json"] = json_data

            res = await self.client.request(
                method=method,
                url=endpoint,
                headers=req_headers if req_headers else None,
                **req_kwargs,
            )
            t_recv = time.perf_counter_ns()
            trace.mark_stage("response_received")

            # 3. Inform rate limiter of server health
            self.rate_limiter.on_response(res.status_code, dict(res.headers))

            # 4. Parse content
            content_type = res.headers.get("content-type", "")
            if "application/json" in content_type:
                try:
                    body = res.json()
                except Exception:
                    body = res.text
            else:
                body = res.text

            trace.complete(success=(res.status_code < 400))
            return {
                "status_code": res.status_code,
                "body": body,
                "headers": dict(res.headers),
                "latency_breakdown": trace.get_breakdown(),
                "network_ms": round((t_recv - t_dispatch) / 1_000_000.0, 3),
            }

        except Exception as e:
            trace.complete(success=False, error=str(e))
            return {
                "status_code": 0,
                "error": str(e),
                "latency_breakdown": trace.get_breakdown(),
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

