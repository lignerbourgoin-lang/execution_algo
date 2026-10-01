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
import random
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
    burst_retries: int = 5                 # Rapid micro-burst attempts at T0 if server hasn't opened gates yet
    burst_interval_ms: float = 80.0       # Delay between micro-burst attempts (ms)


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
        browser_worker: Optional[Any] = None,
    ):
        self.config = config
        self.client = http_client
        self.ntp = ntp_client or NtpClient()
        self.scheduler = HighPrecisionScheduler(self.ntp)
        self.telemetry = telemetry or LatencyTracker()
        self.browser_worker = browser_worker
        self.is_armed = False
        self.active_cart: Optional[CartReservation] = None
        self._prebuilt_requests: Dict[str, httpx.Request] = {}
        self._prebuilt_burst_requests: Dict[str, List[httpx.Request]] = {}

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
        Pre-generates complete burst queues with distinct idempotency keys.
        Guarantees zero-overhead JSON serialization, header parsing, and UUID entropy syscalls at T0.
        """
        headers = self._prepare_headers()
        categories = [self.config.category_id] + self.config.fallback_categories

        for cat_idx, cat in enumerate(categories):
            is_primary = (cat_idx == 0)
            burst_count = (1 + self.config.burst_retries) if is_primary else 1
            burst_requests: List[httpx.Request] = []

            for attempt in range(burst_count):
                action_id = f"ticket_{self.config.event_id}_{cat}_b{attempt}_{uuid.uuid4().hex[:8]}"
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
                burst_requests.append(req)

            self._prebuilt_burst_requests[cat] = burst_requests
            if burst_requests:
                self._prebuilt_requests[cat] = burst_requests[0]

    async def initialize(self):
        """
        Pre-warms TCP/TLS connection and synchronizes high-precision atomic clock.
        Warms origin API endpoint route if supported to exercise true backend path.
        """
        # 1. Non-blocking NTP sync
        await self.ntp.sync_async()

        # 2. Pre-warm HTTP/2 socket targeting the event API route
        api_probe_path = f"/api/events/{self.config.event_id}/availability"
        if hasattr(self.client, "start"):
            try:
                await self.client.start(probe_path=api_probe_path)
            except TypeError:
                await self.client.start()

        # 3. Pre-build binary requests
        self.prebuild_reservation_requests()
        self.is_armed = True

    # [FEATURE: FLEXIBLE_SCHEDULING_DISPATCH] Support orchestrated external scheduling and custom target timestamps
    # Raison: Prevents staggered orchestrator tiers from collapsing onto T0 or double-waiting internally.
    # Attention: skip_scheduling=True must only be set when the caller manages sub-millisecond dispatch.
    async def execute_drop(
        self,
        skip_scheduling: bool = False,
        custom_target_utc: Optional[float] = None,
    ) -> ExecutionResult:
        """
        Executes drop reservation at the exact scheduled millisecond.
        If skip_scheduling is True, bypasses internal scheduler wait and fires immediately.
        If custom_target_utc is provided, coordinates against that specific target epoch.
        Includes automatic 15-second drift resync, GC freeze, and CPU affinity pinning.
        """
        if not self.is_armed:
            await self.initialize()

        now = time.time()
        target_utc = custom_target_utc or self.config.drop_time_utc

        # Scheduled wait if drop time is configured and scheduling is enabled
        if not skip_scheduling and target_utc and target_utc > now:
            # If drop is more than 30s away, do a fine drift resync at T-15s
            time_until_drop = target_utc - now
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
                    target_atomic_timestamp_utc=target_utc,
                    latency_advance_ms=self.config.lead_time_ms,
                )
                return await self._execute_with_fallbacks()

        with freeze_garbage_collection():
            return await self._execute_with_fallbacks()

    async def _execute_with_fallbacks(self) -> ExecutionResult:
        """
        Executes the primary category reservation with rapid micro-burst retries.
        If the server indicates gates are not open yet (404, 425, 503, or 400 not started),
        fires rapid micro-burst retries on the pre-warmed socket up to burst_retries times.
        If sold out (409, 400/422 sold out), immediately cascades to fallback categories.
        """
        categories = [self.config.category_id] + self.config.fallback_categories
        last_result: Optional[ExecutionResult] = None

        for cat_idx, cat in enumerate(categories):
            is_primary = (cat_idx == 0)
            max_attempts = (1 + self.config.burst_retries) if is_primary else 1
            prebuilt_burst_list = self._prebuilt_burst_requests.get(cat, [])

            for attempt in range(max_attempts):
                # Retrieve pre-allocated zero-overhead binary request if available
                prebuilt_req = prebuilt_burst_list[attempt] if attempt < len(prebuilt_burst_list) else None
                extracted_key = prebuilt_req.headers.get("idempotency-key") if prebuilt_req is not None else None
                action_id = extracted_key if extracted_key else f"ticket_{self.config.event_id}_{cat}_b{attempt}_{uuid.uuid4().hex[:8]}"


                signal = Signal(
                    source="ticket_engine",
                    target_id=self.config.event_id,
                    action="RESERVE_TICKETS",
                    payload={
                        "event_id": self.config.event_id,
                        "category_id": cat,
                        "quantity": self.config.quantity,
                        "auth_token": self.config.auth_token,
                        "idempotency_key": action_id,
                        "_prebuilt_request": prebuilt_req,
                    },
                    urgency=3,
                )

                res = await self.execute(signal)
                if res.success:
                    return res

                last_result = res
                status = res.status_code
                err_text = (str(res.error or "") + " " + str(res.data or "")).lower()

                # Case 1: Server not opened yet / drop lag
                # (404 Not Found, 425 Too Early, 503 Service Unavailable, or 400 with 'not open' / 'soon' / 'closed' / 'attente')
                is_unopened = (
                    status in (404, 425, 503) or
                    (status == 400 and any(kw in err_text for kw in ["not open", "not started", "soon", "attente", "ferme", "early"]))
                )

                if is_unopened and attempt < max_attempts - 1:
                    logger.info(
                        f"Drop gate not open yet (HTTP {status}) for '{cat}', micro-burst retry #{attempt + 1}/{self.config.burst_retries} in {self.config.burst_interval_ms}ms..."
                    )
                    await asyncio.sleep(self.config.burst_interval_ms / 1000.0)
                    continue

                # Case 2: Category is sold out (409 Conflict, or 400/422 with 'sold out', 'epuise', 'complet', 'no seats')
                # In this case, do NOT burst retry this sold-out category; break burst and cascade to next category
                if status in (400, 404, 409, 422):
                    logger.info(f"Category '{cat}' unavailable (HTTP {status}), checking next fallback tier...")
                    break
                else:
                    # Fatal error (e.g. 401 Unauthorized, 403 Forbidden)
                    break

        return last_result or ExecutionResult(
            action_id=f"ticket_{uuid.uuid4().hex[:8]}",
            success=False,
            status_code=0,
            data={},
            latency_ms=0.0,
            error="All ticket categories failed",
        )

    async def execute_parallel_categories(
        self,
        categories: Optional[List[str]] = None,
        stagger_delay_ms: float = 0.0,
    ) -> ExecutionResult:
        """
        Executes reservation requests for multiple categories concurrently over the pre-warmed connection.
        First successful reservation (HTTP 200/201) wins, and active_cart is secured.
        stagger_delay_ms allows prioritizing primary categories with a small head start.
        """
        target_cats = categories or ([self.config.category_id] + self.config.fallback_categories)
        if not target_cats:
            return ExecutionResult(
                action_id=f"ticket_{uuid.uuid4().hex[:8]}",
                success=False,
                status_code=0,
                data={},
                latency_ms=0.0,
                error="No categories specified",
            )

        async def _attempt_cat(cat: str, delay_ms: float) -> ExecutionResult:
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)
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
            return await self.execute(signal)

        tasks = [
            asyncio.create_task(_attempt_cat(cat, idx * stagger_delay_ms))
            for idx, cat in enumerate(target_cats)
        ]

        last_res: Optional[ExecutionResult] = None
        for completed_task in asyncio.as_completed(tasks):
            res = await completed_task
            if res.success:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                return res
            last_res = res

        return last_res or ExecutionResult(
            action_id=f"ticket_{uuid.uuid4().hex[:8]}",
            success=False,
            status_code=0,
            data={},
            latency_ms=0.0,
            error="All parallel category attempts failed",
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

        prebuilt = payload.get("_prebuilt_request") or self._prebuilt_requests.get(category)
        if self.browser_worker and getattr(self.browser_worker, "is_running", False):
            endpoint = payload.get("reserve_endpoint", f"/api/events/{self.config.event_id}/reserve")
            reservation_body = {
                "event_id": self.config.event_id,
                "category_id": category,
                "quantity": self.config.quantity,
            }
            target_url = f"{self.config.target_url.rstrip('/')}{endpoint}"
            raw_res = await self.browser_worker.execute_in_browser_fetch(
                endpoint_url=target_url,
                method="POST",
                payload=reservation_body,
                custom_headers=self._prepare_headers(),
            )
            res = {
                "status_code": raw_res.get("status_code", 0),
                "body": raw_res.get("data", {}),
                "error": raw_res.get("error"),
            }
        elif prebuilt is not None:
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
        categories: Optional[List[str]] = None,
        wave_windows_sec: Optional[List[tuple[float, float]]] = None,
        wave_poll_interval_sec: float = 0.15,
        jitter_ms: float = 25.0,
    ) -> Optional[ExecutionResult]:
        """
        Cart Release Sniping (Rattrapage multi-vagues des paniers expirés):
        Polls availability for primary and fallback categories when unpurchased carts expire.
        
        - Automatically switches to fast burst mode (wave_poll_interval_sec) during wave windows
          (e.g. at 10 min [570-660s] and 15 min [870-960s] marks).
        - Applies pseudo-random jitter to prevent cyclic rate-limiting.
        - Respects HTTP 429 Retry-After headers automatically.
        - Snipes returned inventory the millisecond it appears.
        """
        start_time = time.time()
        watched_cats = categories or ([self.config.category_id] + self.config.fallback_categories)
        if not wave_windows_sec:
            # Default typical ticketing cart expiration cycles: 10 min (570-660s) and 15 min (870-960s)
            wave_windows_sec = [(570.0, 660.0), (870.0, 960.0)]

        logger.info(
            f"Starting cart release monitor for event {self.config.event_id} "
            f"(categories: {watched_cats}, duration: {max_duration_sec}s)..."
        )
        headers = self._prepare_headers()
        cycle_deadline_monotonic = time.perf_counter()

        while time.time() - start_time < max_duration_sec:
            cycle_start_monotonic = time.perf_counter()
            elapsed = time.time() - start_time
            in_wave = any(w_start <= elapsed <= w_end for w_start, w_end in wave_windows_sec)
            current_interval = wave_poll_interval_sec if in_wave else poll_interval_sec

            cycle_jitter_sec = (random.uniform(-jitter_ms, jitter_ms)) / 1000.0 if jitter_ms > 0 else 0.0
            target_cycle_interval_sec = max(0.005, current_interval + cycle_jitter_sec)
            cycle_deadline_monotonic = max(cycle_deadline_monotonic + target_cycle_interval_sec, cycle_start_monotonic + target_cycle_interval_sec)

            rate_limited = False
            for cat in watched_cats:
                check_endpoint = f"/api/events/{self.config.event_id}/availability?cat={cat}"
                res = await self.client.execute_fast(
                    method="GET",
                    endpoint=check_endpoint,
                    action_id=f"poll_{uuid.uuid4().hex[:6]}",
                    headers=headers,
                )

                status = res.get("status_code", 0)

                # Rate limiting awareness (429)
                if status == 429:
                    retry_after = float(res.get("headers", {}).get("retry-after", 2.0))
                    logger.warning(f"HTTP 429 Rate limited on availability check. Backing off for {retry_after}s.")
                    await asyncio.sleep(retry_after)
                    cycle_deadline_monotonic = time.perf_counter()
                    rate_limited = True
                    break

                if status == 200:
                    body = res.get("body", {})
                    available = body.get("available", 0) or body.get("seats_left", 0)
                    if available >= self.config.quantity:
                        logger.info(f"[CART RELEASE DETECTED] {available} seats found in category '{cat}'! Executing instant reservation...")
                        signal = Signal(
                            source="ticket_engine",
                            target_id=self.config.event_id,
                            action="RESERVE_TICKETS",
                            payload={
                                "event_id": self.config.event_id,
                                "category_id": cat,
                                "quantity": self.config.quantity,
                                "auth_token": self.config.auth_token,
                                "idempotency_key": f"ticket_release_{self.config.event_id}_{cat}_{uuid.uuid4().hex[:8]}",
                            },
                            urgency=3,
                        )
                        reserve_res = await self.execute(signal)
                        if reserve_res.success:
                            return reserve_res

            # Deadline-based drift-free sleep to ensure strict cycle cadence
            if not rate_limited:
                remaining_cycle_sleep_sec = cycle_deadline_monotonic - time.perf_counter()
                if remaining_cycle_sleep_sec > 0.001:
                    await asyncio.sleep(remaining_cycle_sleep_sec)
                else:
                    await asyncio.sleep(0.001)

        logger.info("Cart release monitoring window expired.")
        return None

    async def shutdown(self):
        self.is_armed = False
        await self.client.close()
