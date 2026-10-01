"""
Ticket Drop & Cart Release Sniping Engine
----------------------------------------
Specialized execution engine dedicated to ticketing and event drops:
1. Exact-millisecond drop execution with synchronized atomic clock and pre-warmed sockets.
2. Cart holding detection (retrieves reservation token and expiration timeout).
3. Cart release sniper: automatically catches tickets released back into the pool when carts expire (10-15 min after drop).
4. Direct browser hand-off for 3D Secure / payment completion.
"""

import asyncio
from dataclasses import dataclass, field
import logging
import time
from typing import Any, Dict, List, Optional
import uuid
import webbrowser

from core.engine.base import BaseExecutor, ExecutionResult, Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.telemetry.tracker import LatencyTracker
from modules.retail.clock.ntp_sync import HighPrecisionScheduler, NtpClient

logger = logging.getLogger("execution.retail.tickets")


@dataclass
class TicketConfig:
    platform_name: str
    target_url: str
    event_id: str
    category_id: str
    quantity: int = 1
    max_price_per_ticket: Optional[float] = None
    drop_time_utc: Optional[float] = None  # Epoch timestamp for drop, or None for immediate
    lead_time_ms: float = 35.0             # Advance firing time based on round-trip latency
    auth_token: Optional[str] = None
    session_cookies: Optional[Dict[str, str]] = None
    auto_open_browser: bool = True


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
    Handles clock synchronization, connection warming, atomic trigger, and cart handoff.
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

    async def initialize(self):
        """
        Pre-warms TCP/TLS connection and synchronizes high-precision atomic clock.
        """
        # 1. Non-blocking NTP sync
        await self.ntp.sync_async()

        # 2. Pre-warm HTTP/2 socket
        await self.client.start()
        self.is_armed = True

    async def execute_drop(self) -> ExecutionResult:
        """
        Executes drop reservation at the exact scheduled millisecond.
        If drop_time_utc is specified, cooperatively waits until target time minus lead time.
        """
        if not self.is_armed:
            await self.initialize()

        # Scheduled wait if drop time is configured
        if self.config.drop_time_utc and self.config.drop_time_utc > time.time():
            await self.scheduler.wait_until_atomic_timestamp(
                target_atomic_timestamp_utc=self.config.drop_time_utc,
                latency_advance_ms=self.config.lead_time_ms,
            )

        signal = Signal(
            source="ticket_engine",
            target_id=self.config.event_id,
            action="RESERVE_TICKETS",
            payload={
                "event_id": self.config.event_id,
                "category_id": self.config.category_id,
                "quantity": self.config.quantity,
                "auth_token": self.config.auth_token,
                "idempotency_key": f"ticket_{self.config.event_id}_{uuid.uuid4().hex[:10]}",
            },
            urgency=3,
        )

        return await self.execute(signal)

    async def execute(self, signal: Signal) -> ExecutionResult:
        """
        Dispatches the ticket reservation request over the warm keep-alive socket.
        """
        payload = signal.payload
        action_id = payload.get("idempotency_key", f"ticket_{uuid.uuid4().hex[:10]}")
        idempotency_key = action_id

        trace = self.telemetry.start_trace(
            action_id=action_id,
            target=self.config.target_url,
            event_id=self.config.event_id,
            category=self.config.category_id,
        )

        headers = {}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"
        if self.config.session_cookies:
            cookie_header = "; ".join([f"{k}={v}" for k, v in self.config.session_cookies.items()])
            headers["Cookie"] = cookie_header

        reservation_body = {
            "event_id": self.config.event_id,
            "category_id": self.config.category_id,
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
            category_id=self.config.category_id,
            quantity=self.config.quantity,
            expires_at_epoch=time.time() + hold_time_sec,
            checkout_url=checkout_url,
            reserved_at_ms=trace.total_latency_ms,
        )

        trace.complete(success=True)

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
                    "category": self.config.category_id,
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

        headers = {}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"

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
