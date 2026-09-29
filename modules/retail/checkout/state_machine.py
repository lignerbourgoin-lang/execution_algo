"""
Asynchronous Checkout State Machine
-----------------------------------
Manages the fast sequential stages of an inventory reservation & checkout pipeline:
- Pre-loads and pre-serializes user profile payloads (addresses, payment tokens) in memory.
- Transitions through states (IDLE -> ARMED -> RESERVED -> SHIPPING -> FINALIZE).
- Minimizes processing overhead during the execution critical path.
"""

import asyncio
from enum import Enum, auto
import time
from typing import Any, Dict, Optional

from core.engine.base import BaseExecutor, ExecutionResult, Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.telemetry.tracker import LatencyTracker


class CheckoutState(Enum):
    IDLE = auto()
    ARMED = auto()
    RESERVING = auto()
    RESERVED = auto()
    SUBMITTING_DETAILS = auto()
    FINALIZING = auto()
    COMPLETED = auto()
    FAILED = auto()


class CheckoutProfile:
    """Stores pre-serialized payloads to eliminate JSON serialization during execution."""

    def __init__(
        self,
        email: str,
        shipping_address: Dict[str, Any],
        payment_token: Optional[str] = None,
    ):
        self.email = email
        self.shipping_address = shipping_address
        self.payment_token = payment_token
        # Pre-serialized dictionaries for immediate sending
        self.preserialized_shipping = {
            "email": email,
            "shipping_address": shipping_address,
        }


class FastCheckoutStateMachine(BaseExecutor):
    """
    Executes transaction sequences over pre-warmed HTTP sockets.
    """

    def __init__(
        self,
        target_domain: str,
        http_client: PrewarmedHttpClient,
        profile: CheckoutProfile,
        telemetry: Optional[LatencyTracker] = None,
    ):
        self.target_domain = target_domain
        self.client = http_client
        self.profile = profile
        self.telemetry = telemetry or LatencyTracker()
        self.state = CheckoutState.IDLE
        self.reservation_token: Optional[str] = None

    async def initialize(self):
        """Pre-warm connection and enter ARMED state."""
        await self.client.start()
        self.state = CheckoutState.ARMED

    async def execute(self, signal: Signal) -> ExecutionResult:
        """
        Executes reservation upon receiving trigger signal.
        """
        t_start = time.perf_counter_ns()
        trace = self.telemetry.start_trace(
            action_id=f"checkout_{signal.target_id}",
            target=self.target_domain,
            item_id=signal.payload.get("item_id"),
        )

        self.state = CheckoutState.RESERVING

        # Step 1: Reserve Item / Add to Cart
        reserve_method = signal.payload.get("reserve_method", "POST")
        reserve_res = await self.client.execute_fast(
            method=reserve_method,
            endpoint=signal.payload.get("reserve_endpoint", "/api/cart/add"),
            action_id="reserve_item",
            json_data={
                "item_id": signal.payload.get("item_id"),
                "quantity": signal.payload.get("quantity", 1),
            } if reserve_method == "POST" else None,
        )
        trace.mark_stage("item_reservation_ack")

        if reserve_res.get("status_code") not in (200, 201):
            self.state = CheckoutState.FAILED
            trace.complete(success=False, error="Reservation failed")
            return ExecutionResult(
                action_id=trace.action_id,
                success=False,
                status_code=reserve_res.get("status_code", 0),
                data=reserve_res,
                latency_ms=trace.total_latency_ms,
                error="Failed to lock inventory",
            )

        self.state = CheckoutState.RESERVED
        self.reservation_token = (
            reserve_res.get("body", {}).get("token")
            if isinstance(reserve_res.get("body"), dict)
            else "tok_simulated"
        )

        # Step 2: Inject pre-serialized shipping details
        self.state = CheckoutState.SUBMITTING_DETAILS
        shipping_payload = dict(self.profile.preserialized_shipping)
        shipping_payload["token"] = self.reservation_token

        shipping_method = signal.payload.get("shipping_method", "POST")
        shipping_res = await self.client.execute_fast(
            method=shipping_method,
            endpoint=signal.payload.get("shipping_endpoint", "/api/checkout/shipping"),
            action_id="submit_shipping",
            json_data=shipping_payload if shipping_method == "POST" else None,
        )
        trace.mark_stage("shipping_submitted_ack")

        is_success = (shipping_res.get("status_code") in (200, 201))
        self.state = CheckoutState.COMPLETED if is_success else CheckoutState.FAILED
        trace.complete(success=is_success, error=None if is_success else f"HTTP {shipping_res.get('status_code')}")

        return ExecutionResult(
            action_id=trace.action_id,
            success=is_success,
            status_code=shipping_res.get("status_code", 0),
            data={
                "reservation": reserve_res.get("body"),
                "shipping": shipping_res.get("body"),
                "stages": trace.get_breakdown(),
            },
            latency_ms=trace.total_latency_ms,
            error=None if is_success else f"Shipping step returned HTTP {shipping_res.get('status_code')}",
        )

    async def shutdown(self):
        self.state = CheckoutState.IDLE
        await self.client.close()
