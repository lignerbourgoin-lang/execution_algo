"""
Ticket Drop & Cart Release Sniping Engine
----------------------------------------
Specialized execution engine dedicated to ticketing and event drops:
1. Zero-allocation exact-millisecond drop execution with synchronized atomic clock and pre-warmed sockets.
2. GC freeze and CPU affinity pinning during the critical T0 firing window.
3. Multi-category cascading fallback (if Tier 1 is sold out, auto-snipes Tier 2 in < 3 ms).
4. Continuous NTP drift resync 15 seconds before scheduled drops.
5. Cart holding detection with audible alert and browser hand-off for 3D Secure / payment.
6. Cart release sniper: catches tickets released back into pool when carts expire.
"""

import asyncio
from dataclasses import dataclass, field
import logging
import time
from typing import Any, Dict, List, Optional
import uuid
import webbrowser

import httpx

from core.engine.base import BaseExecutor, ExecutionResult, Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.system import freeze_garbage_collection, pin_thread_to_cpu, play_success_alert
from core.telemetry.tracker import LatencyTracker
from modules.retail.clock.ntp_sync import HighPrecisionScheduler, NtpClient

logger = logging.getLogger("execution.retail.tickets")


@dataclass
class TicketConfig:
    platform_name: str
    target_url: str
    event_id: str
    category_id: str
    fallback_categories: List[str] = field(default_factory=list)  # Alternative categories if primary is sold out
    quantity: int = 1
    max_price_per_ticket: Optional[float] = None
    drop_time_utc: Optional[float] = None  # Epoch timestamp for drop, or None for immediate
    lead_time_ms: float = 35.0             # Advance firing time based on round-trip latency
    auth_token: Optional[str] = None
    session_cookies: Optional[Dict[str, str]] = None
    auto_open_browser: bool = True
    audible_alert: bool = True


@dataclass
class CartReservation:
    token: str
    event_id: str
    category_id: str
    quantity: int
    expires_at_epoch: float
    checkout_url: str
    reserved_at_ms: float


class TicketDropExecutor(BaseExecutor):
    """
    Precision execution engine for ticketing drops.
    Handles clock synchronization, connection warming, atomic trigger, cascading fallback, and cart handoff.
    """

    def __init__(
        self,
        config: TicketConfig,
        http_client: PrewarmedHttpClient,
        ntp_client: Optional[NtpClient] = None,
        telemetry: Optional[LatencyTracker] = None,
    ):
        self.config = config
        self.client = http_client
        self.ntp = ntp_client or NtpClient()
        self.scheduler = HighPrecisionScheduler(self.ntp)
        self.telemetry = telemetry or LatencyTracker()
        self.is_armed = False
        self.active_cart: Optional[CartReservation] = None
        self._prebuilt_requests: Dict[str, httpx.Request] = {}

    def _prepare_headers(self) -> Dict[str, str]:
        headers = {}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"
        if self.config.session_cookies:
            cookie_header = "; ".join([f"{k}={v}" for k, v in self.config.session_cookies.items()])
            headers["Cookie"] = cookie_header
        return headers

    def prebuild_reservation_requests(self):
        """
        Pre-constructs binary HTTP request objects for primary and fallback categories.
        Guarantees zero-overhead JSON serialization, header parsing, or URL encoding at T0.
        """
        headers = self._prepare_headers()
        categories = [self.config.category_id] + self.config.fallback_categories

        for cat in categories:
            action_id = f"ticket_{self.config.event_id}_{cat}_{uuid.uuid4().hex[:8]}"
            req = self.client.build_fast_request(
                method="POST",
                endpoint=f"/api/events/{self.config.event_id}/reserve",
                json_data={
                    "event_id": self.config.event_id,
                    "category_id": cat,
                    "quantity": self.config.quantity,
                },
                headers=headers,
                idempotency_key=action_id,
            )
            self._prebuilt_requests[cat] = req

    async def initialize(self):
        """
        Pre-warms TCP/TLS connection and synchronizes high-precision atomic clock.
        """
        # 1. Non-blocking NTP sync
        await self.ntp.sync_async()

        # 2. Pre-warm HTTP/2 socket
        await self.client.start()

        # 3. Pre-build binary requests
        self.prebuild_reservation_requests()
        self.is_armed = True

    async def execute_drop(self) -> ExecutionResult:
        """
        Executes drop reservation at the exact scheduled millisecond.
        Includes automatic 15-second drift resync, GC freeze, and CPU affinity pinning.
        """
        if not self.is_armed:
            await self.initialize()

        now = time.time()
        # Scheduled wait if drop time is configured
        if self.config.drop_time_utc and self.config.drop_time_utc > now:
            # If drop is more than 30s away, do a fine drift resync at T-15s
            time_until_drop = self.config.drop_time_utc - now
            if time_until_drop > 30.0:
                await asyncio.sleep(time_until_drop - 15.0)
                try:
                    await self.ntp.sync_async()
                    logger.info("Refreshed atomic clock drift at T-15s.")
                except Exception as e:
                    logger.warning(f"T-15s NTP drift refresh skipped: {e}")

            # Pin thread to high-performance CPU core before the fine wait
            pin_thread_to_cpu(core_index=2)

            # Freeze Python GC for the final millisecond firing path
            with freeze_garbage_collection():
                await self.scheduler.wait_until_atomic_timestamp(
                    target_atomic_timestamp_utc=self.config.drop_time_utc,
                    latency_advance_ms=self.config.lead_time_ms,
                )
                return await self._execute_with_fallbacks()

        with freeze_garbage_collection():
            return await self._execute_with_fallbacks()

    async def _execute_with_fallbacks(self) -> ExecutionResult:
        """
        Executes the primary category reservation.
        If sold out (400, 404, 409, 422), immediately tries fallback categories on the same socket.
        """
        categories = [self.config.category_id] + self.config.fallback_categories
        last_result: Optional[ExecutionResult] = None

        for cat in categories:
            signal = Signal(
                source="ticket_engine",
                target_id=self.config.event_id,
                action="RESERVE_TICKETS",
                payload={
                    "event_id": self.config.event_id,
                    "category_id": cat,
                    "quantity": self.config.quantity,
                    "auth_token": self.config.auth_token,
                    "idempotency_key": f"ticket_{self.config.event_id}_{cat}_{uuid.uuid4().hex[:8]}",
                },
                urgency=3,
            )

            res = await self.execute(signal)
            if res.success:
                return res

            last_result = res
            # If server indicates sold out or invalid category, attempt next fallback immediately
            if res.status_code in (400, 404, 409, 422):
                logger.info(f"Category '{cat}' unavailable (HTTP {res.status_code}), attempting next tier...")
                continue
            else:
                # Fatal network error or server down, stop
                break

        return last_result or ExecutionResult(
            action_id=f"ticket_{uuid.uuid4().hex[:8]}",
            success=False,
            status_code=0,
            data={},
            latency_ms=0.0,
            error="All ticket categories failed",
        )

    async def execute(self, signal: Signal) -> ExecutionResult:
        """
        Dispatches the ticket reservation request over the warm keep-alive socket.
        Uses pre-built request when available for zero-allocation firing.
        """
        payload = signal.payload
        category = payload.get("category_id", self.config.category_id)
        action_id = payload.get("idempotency_key", f"ticket_{uuid.uuid4().hex[:10]}")
        idempotency_key = action_id

        trace = self.telemetry.start_trace(
            action_id=action_id,
            target=self.config.target_url,
            event_id=self.config.event_id,
            category=category,
        )

        prebuilt = self._prebuilt_requests.get(category)
        if prebuilt is not None:
            res = await self.client.send_fast(request=prebuilt, action_id=action_id)
        else:
            headers = self._prepare_headers()
            reservation_body = {
                "event_id": self.config.event_id,
                "category_id": category,
                "quantity": self.config.quantity,
            }
            endpoint = payload.get("reserve_endpoint", f"/api/events/{self.config.event_id}/reserve")
            res = await self.client.execute_fast(
                method="POST",
                endpoint=endpoint,
                action_id=action_id,
                json_data=reservation_body,
                headers=headers,
                idempotency_key=idempotency_key,
            )

        trace.mark_stage("ticket_reservation_ack")
        status_code = res.get("status_code", 0)
        success = (status_code in (200, 201))

        if not success:
            err_msg = res.get("error") or f"HTTP {status_code}"
            trace.complete(success=False, error=err_msg)
            return ExecutionResult(
                action_id=action_id,
                success=False,
                status_code=status_code,
                data=res.get("body", {}),
                latency_ms=trace.total_latency_ms,
                error=err_msg,
            )

        body = res.get("body", {})
        token = body.get("token") or body.get("cart_token") or body.get("reservation_id") or action_id
        checkout_url = body.get("checkout_url") or f"{self.config.target_url}/checkout?cart={token}"
        hold_time_sec = float(body.get("hold_time_sec", 600.0))  # Default 10 min hold

        self.active_cart = CartReservation(
            token=str(token),
            event_id=self.config.event_id,
            category_id=category,
            quantity=self.config.quantity,
            expires_at_epoch=time.time() + hold_time_sec,
            checkout_url=checkout_url,
            reserved_at_ms=trace.total_latency_ms,
        )

        trace.complete(success=True)

        # Audible alert for operator
        if self.config.audible_alert:
            play_success_alert()

        # Auto-open browser for 3D Secure / Payment completion if requested
        if self.config.auto_open_browser and checkout_url.startswith("http"):
            try:
                webbrowser.open(checkout_url)
            except Exception as e:
                logger.warning(f"Could not open browser: {e}")

        return ExecutionResult(
            action_id=action_id,
            success=True,
            status_code=status_code,
            data={
                "cart": {
                    "token": self.active_cart.token,
                    "checkout_url": self.active_cart.checkout_url,
                    "expires_in_sec": hold_time_sec,
                    "quantity": self.config.quantity,
                    "category": category,
                },
                "latency_breakdown": trace.get_breakdown(),
            },
            latency_ms=trace.total_latency_ms,
        )

    async def monitor_cart_releases(
        self,
        poll_interval_sec: float = 0.5,
        max_duration_sec: float = 900.0,
    ) -> Optional[ExecutionResult]:
        """
        Cart Release Sniping:
        Polls the availability endpoint rapidly when carts expire (10-15 min after drop)
        to instantly snatch any returned inventory.
        """
        start_time = time.time()
        logger.info(f"Starting cart release monitor for event {self.config.event_id} (interval {poll_interval_sec}s)...")

        headers = self._prepare_headers()

        while time.time() - start_time < max_duration_sec:
            check_endpoint = f"/api/events/{self.config.event_id}/availability?cat={self.config.category_id}"
            res = await self.client.execute_fast(
                method="GET",
                endpoint=check_endpoint,
                action_id=f"poll_{uuid.uuid4().hex[:6]}",
                headers=headers,
            )

            if res.get("status_code") == 200:
                body = res.get("body", {})
                available = body.get("available", 0) or body.get("seats_left", 0)
                if available >= self.config.quantity:
                    logger.info(f"[CART RELEASE DETECTED] {available} seats found! Executing instant reservation...")
                    return await self.execute_drop()

            await asyncio.sleep(poll_interval_sec)

        logger.info("Cart release monitoring window expired.")
        return None

    async def shutdown(self):
        self.is_armed = False
        await self.client.close()
