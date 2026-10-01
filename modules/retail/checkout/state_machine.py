"""
Asynchronous Checkout State Machine
-----------------------------------
Manages the sequential stages of an inventory reservation & checkout pipeline:
- Pre-builds the user profile payload (addresses, payment tokens) in memory.
- Transitions through states (IDLE -> ARMED -> RESERVING -> RESERVED -> SUBMITTING_DETAILS -> COMPLETED).
- Single-flight: one checkout at a time per machine, and no second purchase after a success.
"""

import asyncio
import logging
import uuid
from enum import Enum, auto
from typing import Any, Dict, Optional

from core.engine.base import BaseExecutor, ExecutionResult, Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.telemetry.tracker import LatencyTracker

logger = logging.getLogger("modules.retail.checkout")

METHODS_WITH_BODY = frozenset({"POST", "PUT", "PATCH"})
DEFAULT_TOKEN_FIELD = "token"


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
    """Holds the shipping payload built once, outside of the critical path."""

    def __init__(
        self,
        email: str,
        shipping_address: Dict[str, Any],
        payment_token: Optional[str] = None,
    ):
        self.email = email
        self.shipping_address = shipping_address
        self.payment_token = payment_token
        self.preserialized_shipping: Dict[str, Any] = {
            "email": email,
            "shipping_address": shipping_address,
        }
        if payment_token:
            self.preserialized_shipping["payment_token"] = payment_token


def _is_success_status(status_code: int) -> bool:
    return 200 <= status_code < 300


class FastCheckoutStateMachine(BaseExecutor):
    """
    Executes transaction sequences over pre-warmed HTTP sockets.

    Signal payload keys:
      item_id, quantity, reserve_endpoint, reserve_method, shipping_endpoint, shipping_method,
      token_field (default "token"), require_reservation_token (default True),
      idempotency_key (optional: pass the same key when retrying the same purchase).
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
        self._execution_lock = asyncio.Lock()

    async def initialize(self):
        """Pre-warm connection and enter ARMED state."""
        await self.client.start()
        self.state = CheckoutState.ARMED

    def reset(self):
        """Explicitly re-arms the machine after a COMPLETED purchase (operator decision only)."""
        self.state = CheckoutState.ARMED
        self.reservation_token = None

    def _rejected(self, action_id: str, error: str) -> ExecutionResult:
        logger.warning("Checkout %s rejected: %s", action_id, error)
        return ExecutionResult(action_id=action_id, success=False, status_code=0, data={}, latency_ms=0.0, error=error)

    async def execute(self, signal: Signal) -> ExecutionResult:
        """Executes the reservation then shipping steps upon a trigger signal."""
        action_id = f"checkout_{signal.target_id}_{uuid.uuid4().hex[:8]}"

        # [FEATURE: CHECKOUT_SINGLE_FLIGHT] Concurrent or repeated triggers never buy twice.
        # Raison: the orchestrator fires one task per signal; two signals for the same drop
        #         used to run two full checkouts in parallel and clobber the shared state.
        # Attention: a second trigger is REJECTED, not queued (a queued run after a success
        #            is a double purchase). After COMPLETED, only reset() re-arms the machine.
        if self._execution_lock.locked():
            return self._rejected(action_id, "Checkout already in flight")
        if self.state == CheckoutState.COMPLETED:
            return self._rejected(action_id, "Checkout already completed; call reset() to buy again")

        async with self._execution_lock:
            return await self._run_checkout(signal, action_id)

    async def _run_checkout(self, signal: Signal, action_id: str) -> ExecutionResult:
        payload = signal.payload
        idempotency_key = payload.get("idempotency_key") or str(uuid.uuid4())
        token_field = payload.get("token_field", DEFAULT_TOKEN_FIELD)
        require_reservation_token = payload.get("require_reservation_token", True)

        trace = self.telemetry.start_trace(
            action_id=action_id,
            target=self.target_domain,
            item_id=payload.get("item_id"),
            idempotency_key=idempotency_key,
        )
        self.state = CheckoutState.RESERVING
        self.reservation_token = None

        def fail(error: str, status_code: int, data: Dict[str, Any]) -> ExecutionResult:
            self.state = CheckoutState.FAILED
            trace.complete(success=False, error=error)
            logger.warning("Checkout %s failed: %s", action_id, error)
            return ExecutionResult(
                action_id=action_id,
                success=False,
                status_code=status_code,
                data=data,
                latency_ms=trace.total_latency_ms,
                error=error,
            )

        # Step 1: Reserve Item / Add to Cart
        reserve_method = payload.get("reserve_method", "POST").upper()
        reserve_response = await self.client.execute_fast(
            method=reserve_method,
            endpoint=payload.get("reserve_endpoint", "/api/cart/add"),
            action_id=f"{action_id}_reserve",
            json_data={"item_id": payload.get("item_id"), "quantity": payload.get("quantity", 1)}
            if reserve_method in METHODS_WITH_BODY
            else None,
            idempotency_key=f"{idempotency_key}-reserve",
        )
        trace.mark_stage("item_reservation_ack")

        reserve_status = reserve_response.get("status_code", 0)
        if not _is_success_status(reserve_status):
            return fail(f"Reservation failed with HTTP {reserve_status}", reserve_status, reserve_response)

        # [FEATURE: NO_SIMULATED_TOKEN] The reservation token is read from the response, never invented.
        # Raison: the old fallback "tok_simulated" turned a non-JSON answer into a fake success.
        # Attention: cookie-based carts return no token; set require_reservation_token=False for them.
        reserve_body = reserve_response.get("body")
        reservation_token = reserve_body.get(token_field) if isinstance(reserve_body, dict) else None
        if reservation_token is None and require_reservation_token:
            return fail(f"Reservation response has no '{token_field}' field", reserve_status, reserve_response)

        self.state = CheckoutState.RESERVED
        self.reservation_token = str(reservation_token) if reservation_token is not None else None

        # Step 2: Submit the pre-built shipping details
        self.state = CheckoutState.SUBMITTING_DETAILS
        shipping_payload = dict(self.profile.preserialized_shipping)
        if self.reservation_token is not None:
            shipping_payload[token_field] = self.reservation_token

        shipping_method = payload.get("shipping_method", "POST").upper()
        shipping_response = await self.client.execute_fast(
            method=shipping_method,
            endpoint=payload.get("shipping_endpoint", "/api/checkout/shipping"),
            action_id=f"{action_id}_shipping",
            json_data=shipping_payload if shipping_method in METHODS_WITH_BODY else None,
            idempotency_key=f"{idempotency_key}-shipping",
        )
        trace.mark_stage("shipping_submitted_ack")

        shipping_status = shipping_response.get("status_code", 0)
        if not _is_success_status(shipping_status):
            return fail(
                f"Shipping step returned HTTP {shipping_status}",
                shipping_status,
                {"reservation": reserve_body, "shipping": shipping_response},
            )

        self.state = CheckoutState.COMPLETED
        trace.complete(success=True)
        return ExecutionResult(
            action_id=action_id,
            success=True,
            status_code=shipping_status,
            data={
                "reservation": reserve_body,
                "shipping": shipping_response.get("body"),
                "idempotency_key": idempotency_key,
                "stages": trace.get_breakdown(),
            },
            latency_ms=trace.total_latency_ms,
        )

    async def shutdown(self):
        self.state = CheckoutState.IDLE
        await self.client.close()
