"""
Marketplace Fast Buyer & Offer Executor
---------------------------------------
Executes instant purchase / reservation / offer actions over pre-warmed HTTP sockets
for second-hand marketplace platforms.

Features:
- Stateless execution with per-action idempotency keys (prevents duplicate orders).
- Pre-warmed connection persistence for minimum protocol latency.
- Full trace telemetry and latency breakdown.
"""

from dataclasses import dataclass
import logging
from typing import Any, Dict, Optional
import uuid

from core.engine.base import BaseExecutor, ExecutionResult, Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.telemetry.tracker import LatencyTracker

logger = logging.getLogger("execution.marketplace.buyer")


@dataclass
class BuyerProfile:
    user_token: str
    shipping_address_id: Optional[str] = None
    payment_method_id: Optional[str] = None
    buyer_currency: str = "EUR"


class MarketplaceBuyerExecutor(BaseExecutor):
    """
    Executes buy/offer orders on marketplace APIs using persistent HTTP sessions.
    """

    def __init__(
        self,
        base_url: str,
        http_client: PrewarmedHttpClient,
        profile: BuyerProfile,
        telemetry: Optional[LatencyTracker] = None,
    ):
        self.base_url = base_url
        self.client = http_client
        self.profile = profile
        self.telemetry = telemetry or LatencyTracker()
        self.is_ready = False

    async def initialize(self):
        """Pre-warms TLS socket to marketplace API."""
        await self.client.start()
        self.is_ready = True

    async def execute(self, signal: Signal) -> ExecutionResult:
        """
        Executes buy/reserve action on target item.
        """
        payload = signal.payload
        item_id = payload.get("item_id") or signal.target_id
        action_id = f"buy_{item_id}_{uuid.uuid4().hex[:8]}"
        idempotency_key = payload.get("idempotency_key") or str(uuid.uuid4())

        trace = self.telemetry.start_trace(
            action_id=action_id,
            target=self.base_url,
            item_id=item_id,
            idempotency_key=idempotency_key,
        )

        headers = {
            "Authorization": f"Bearer {self.profile.user_token}",
        }

        buy_endpoint = payload.get("buy_endpoint", f"/api/v2/items/{item_id}/buy")
        body = {
            "item_id": item_id,
            "currency": self.profile.buyer_currency,
        }
        if self.profile.shipping_address_id:
            body["shipping_address_id"] = self.profile.shipping_address_id
        if self.profile.payment_method_id:
            body["payment_method_id"] = self.profile.payment_method_id

        res = await self.client.execute_fast(
            method="POST",
            endpoint=buy_endpoint,
            action_id=action_id,
            json_data=body,
            headers=headers,
            idempotency_key=idempotency_key,
        )

        trace.mark_stage("purchase_ack")
        status_code = res.get("status_code", 0)
        success = (status_code in (200, 201))

        error_msg = None
        if not success:
            err = res.get("body")
            if isinstance(err, dict):
                error_msg = err.get("message") or err.get("error") or f"HTTP {status_code}"
            else:
                error_msg = res.get("error") or f"HTTP {status_code}"
            trace.complete(success=False, error=error_msg)
        else:
            trace.complete(success=True)

        return ExecutionResult(
            action_id=action_id,
            success=success,
            status_code=status_code,
            data=res.get("body", {}),
            latency_ms=trace.total_latency_ms,
            error=error_msg,
        )

    async def shutdown(self):
        self.is_ready = False
        await self.client.close()
