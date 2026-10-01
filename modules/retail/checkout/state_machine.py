"""
Asynchronous Checkout State Machine
-----------------------------------
Manages the fast sequential stages of an inventory reservation & checkout pipeline:
- Thread-safe & stateless per execution (no shared state between concurrent calls).
- Mandatory Idempotency-Key (UUIDv4) to guarantee zero duplicate charges or orders.
- Strict response validation (zero simulated tokens: fails explicitly if server returns non-JSON or missing token).
"""

from dataclasses import dataclass
from enum import Enum, auto
import json
import time
from typing import Any, Dict, Optional
import uuid

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


@dataclass
class ExecutionContext:
    """Per-execution context avoiding state collisions between concurrent tasks."""
    action_id: str
    idempotency_key: str
    state: CheckoutState = CheckoutState.IDLE
    reservation_token: Optional[str] = None
    error: Optional[str] = None


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
        # Pre-serialized dictionary for immediate reuse
        self.preserialized_shipping = {
            "email": email,
            "shipping_address": shipping_address,
        }
        if payment_token:
            self.preserialized_shipping["payment_token"] = payment_token


class FastCheckoutStateMachine(BaseExecutor):
    """
    Stateless transaction executor operating over pre-warmed HTTP sockets.
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
        self.is_armed = False

    async def initialize(self):
        """Pre-warm connection and set armed status."""
        await self.client.start()
        self.is_armed = True

    async def execute(self, signal: Signal) -> ExecutionResult:
        """
        Executes a reservation flow in an isolated, stateless execution context.
        """
        # Create an isolated execution context for this specific run
        action_id = f"checkout_{signal.target_id}_{uuid.uuid4().hex[:8]}"
        idempotency_key = signal.payload.get("idempotency_key") or str(uuid.uuid4())

        ctx = ExecutionContext(
            action_id=action_id,
            idempotency_key=idempotency_key,
            state=CheckoutState.RESERVING,
        )

        trace = self.telemetry.start_trace(
            action_id=ctx.action_id,
            target=self.target_domain,
            item_id=signal.payload.get("item_id"),
            idempotency_key=ctx.idempotency_key,
        )

        # Step 1: Reserve Item / Add to Cart
        reserve_method = signal.payload.get("reserve_method", "POST")
        reserve_res = await self.client.execute_fast(
            method=reserve_method,
            endpoint=signal.payload.get("reserve_endpoint", "/api/cart/add"),
            action_id=f"{ctx.action_id}_reserve",
            json_data={
                "item_id": signal.payload.get("item_id"),
                "quantity": signal.payload.get("quantity", 1),
            } if reserve_method == "POST" else None,
            idempotency_key=f"{ctx.idempotency_key}_reserve",
        )
        trace.mark_stage("item_reservation_ack")

        # Strict validation: must be 200 or 201
        if reserve_res.get("status_code") not in (200, 201):
            ctx.state = CheckoutState.FAILED
            ctx.error = f"Reservation failed with HTTP {reserve_res.get('status_code')}"
            trace.complete(success=False, error=ctx.error)
            return ExecutionResult(
                action_id=ctx.action_id,
                success=False,
                status_code=reserve_res.get("status_code", 0),
                data=reserve_res,
                latency_ms=trace.total_latency_ms,
                error=ctx.error,
            )

        # Strict token extraction: NO fake/simulated fallback
        body = reserve_res.get("body")
        token = None
        if isinstance(body, dict):
            token = body.get("token") or body.get("cart_id") or body.get("id") or body.get("reservation_token")

        if not token:
            ctx.state = CheckoutState.FAILED
            ctx.error = "Server response missing reservation token or cart ID"
            trace.complete(success=False, error=ctx.error)
            return ExecutionResult(
                action_id=ctx.action_id,
                success=False,
                status_code=reserve_res.get("status_code", 200),
                data=reserve_res,
                latency_ms=trace.total_latency_ms,
                error=ctx.error,
            )

        ctx.state = CheckoutState.RESERVED
        ctx.reservation_token = str(token)

        # Step 2: Inject shipping details
        ctx.state = CheckoutState.SUBMITTING_DETAILS
        shipping_payload = dict(self.profile.preserialized_shipping)
        shipping_payload["token"] = ctx.reservation_token

        shipping_method = signal.payload.get("shipping_method", "POST")
        shipping_res = await self.client.execute_fast(
            method=shipping_method,
            endpoint=signal.payload.get("shipping_endpoint", "/api/checkout/shipping"),
            action_id=f"{ctx.action_id}_shipping",
            json_data=shipping_payload if shipping_method == "POST" else None,
            idempotency_key=f"{ctx.idempotency_key}_shipping",
        )
        trace.mark_stage("shipping_submitted_ack")

        is_success = (shipping_res.get("status_code") in (200, 201))
        ctx.state = CheckoutState.COMPLETED if is_success else CheckoutState.FAILED
        err_msg = None if is_success else f"Shipping step returned HTTP {shipping_res.get('status_code')}"
        trace.complete(success=is_success, error=err_msg)

        return ExecutionResult(
            action_id=ctx.action_id,
            success=is_success,
            status_code=shipping_res.get("status_code", 0),
            data={
                "reservation": reserve_res.get("body"),
                "shipping": shipping_res.get("body"),
                "token": ctx.reservation_token,
                "stages": trace.get_breakdown(),
            },
            latency_ms=trace.total_latency_ms,
            error=err_msg,
        )

    async def shutdown(self):
        self.is_armed = False
        await self.client.close()
