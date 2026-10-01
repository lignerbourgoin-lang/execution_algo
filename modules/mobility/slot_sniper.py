"""
Mobility & Public Slot Reservation Sniping Module
-------------------------------------------------
Automated rapid slot booking (e.g., driving exams, consular slots, high-speed rail seats).
Monitors slot availability feeds and claims the earliest valid opening with zero human latency.
"""

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Any, Dict, List, Optional
import uuid

from core.engine.base import BaseExecutor, BaseStrategy, ExecutionResult, Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.telemetry.tracker import LatencyTracker

logger = logging.getLogger("execution.mobility.slot")


@dataclass
class SlotRequirement:
    earliest_date: datetime
    latest_date: datetime
    preferred_center_ids: List[str]
    user_id: str
    booking_token: str


class SlotSniperStrategy(BaseStrategy):
    """
    Evaluates available appointment or travel slots against user time bounds.
    Emits an urgent CLAIM_SLOT signal on the first qualifying opportunity.
    """

    def __init__(self, requirement: SlotRequirement):
        self.requirement = requirement

    def evaluate(self, market_data: Dict[str, Any]) -> Optional[Signal]:
        """
        Evaluates a newly opened slot announcement.
        """
        center_id = market_data.get("center_id")
        if self.requirement.preferred_center_ids and center_id not in self.requirement.preferred_center_ids:
            return None

        slot_iso = market_data.get("datetime_iso")
        if not slot_iso:
            return None

        try:
            slot_dt = datetime.fromisoformat(slot_iso)
        except Exception:
            return None

        if slot_dt < self.requirement.earliest_date or slot_dt > self.requirement.latest_date:
            return None

        slot_id = str(market_data.get("slot_id", uuid.uuid4().hex[:8]))

        return Signal(
            source="slot_sniper",
            target_id=slot_id,
            action="CLAIM_SLOT",
            payload={
                "slot_id": slot_id,
                "center_id": center_id,
                "datetime_iso": slot_iso,
                "user_id": self.requirement.user_id,
                "booking_token": self.requirement.booking_token,
            },
            urgency=3,
        )


class SlotBookingExecutor(BaseExecutor):
    """
    Claims the slot over a pre-warmed HTTP connection.
    """

    def __init__(
        self,
        base_url: str,
        http_client: PrewarmedHttpClient,
        telemetry: Optional[LatencyTracker] = None,
    ):
        self.base_url = base_url
        self.client = http_client
        self.telemetry = telemetry or LatencyTracker()
        self.is_ready = False

    async def initialize(self):
        await self.client.start()
        self.is_ready = True

    async def execute(self, signal: Signal) -> ExecutionResult:
        payload = signal.payload
        slot_id = payload.get("slot_id")
        action_id = f"claim_{slot_id}_{uuid.uuid4().hex[:8]}"
        idempotency_key = f"claim_{slot_id}_{payload.get('user_id')}"

        trace = self.telemetry.start_trace(action_id=action_id, target=self.base_url)

        headers = {
            "Authorization": f"Bearer {payload.get('booking_token')}",
        }

        endpoint = payload.get("claim_endpoint", f"/api/slots/{slot_id}/book")
        res = await self.client.execute_fast(
            method="POST",
            endpoint=endpoint,
            action_id=action_id,
            json_data={"user_id": payload.get("user_id")},
            headers=headers,
            idempotency_key=idempotency_key,
        )

        trace.mark_stage("slot_claimed_ack")
        status_code = res.get("status_code", 0)
        success = (status_code in (200, 201))

        error_msg = None if success else res.get("error") or f"HTTP {status_code}"
        trace.complete(success=success, error=error_msg)

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
