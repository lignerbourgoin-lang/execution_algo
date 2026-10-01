"""
TWAP (Time-Weighted Average Price) Execution Algorithm
------------------------------------------------------
Splits a large parent order into smaller child slices distributed evenly
(or with randomized jitter) across a designated time horizon.

Features:
- Sub-slice volume jitter (+/- percentage) to avoid market impact and signature detection.
- Price limit collar: pauses or cancels slice execution if market moves beyond threshold.
- Slippage tracking against the arrival price benchmark.
- Idempotent slice execution via UUIDv4 client order IDs.
"""

from dataclasses import dataclass, field
import logging
import math
import random
import time
from typing import Any, Dict, List, Optional
import uuid

from core.engine.base import BaseExecutor, BaseStrategy, ExecutionResult, Signal

logger = logging.getLogger("execution.finance.twap")


@dataclass
class TWAPConfig:
    symbol: str
    side: str  # "BUY" or "SELL"
    total_quantity: float
    duration_seconds: float
    slices: int
    price_limit: Optional[float] = None
    jitter_pct: float = 0.10  # +/- 10% volume randomization per slice
    time_jitter_sec: float = 0.0  # Optional interval randomization
    order_type: str = "LIMIT"  # "LIMIT" or "MARKET"


@dataclass
class TWAPSlice:
    index: int
    planned_qty: float
    scheduled_time: float
    executed_qty: float = 0.0
    fill_price: float = 0.0
    status: str = "PENDING"  # PENDING, FILLED, SKIPPED_PRICE_LIMIT, FAILED
    client_order_id: str = field(default_factory=lambda: f"twap_{uuid.uuid4().hex[:12]}")
    latency_ms: float = 0.0


class TWAPStrategy(BaseStrategy):
    """
    Evaluates market data ticks and monitors whether the current price
    satisfies the TWAP collar limit before triggering child slices.
    """

    def __init__(self, config: TWAPConfig):
        self.config = config
        self.latest_market_price: Optional[float] = None

    def evaluate(self, market_data: Dict[str, Any]) -> Optional[Signal]:
        price = market_data.get("price") or market_data.get("close")
        if price is not None:
            self.latest_market_price = float(price)

        # Check price limit constraint
        if self.config.price_limit is not None and self.latest_market_price is not None:
            if self.config.side == "BUY" and self.latest_market_price > self.config.price_limit:
                # Market too expensive to buy
                return None
            elif self.config.side == "SELL" and self.latest_market_price < self.config.price_limit:
                # Market too cheap to sell
                return None

        return None


class TWAPExecutor:
    """
    Coordinates the scheduled execution of TWAP child slices.
    Calculates execution benchmark (Arrival Price, TWAP, Slippage).
    """

    def __init__(self, config: TWAPConfig, order_executor: BaseExecutor):
        self.config = config
        self.order_executor = order_executor
        self.slices: List[TWAPSlice] = []
        self.arrival_price: Optional[float] = None
        self.is_running = False
        self._generate_slices()

    def _generate_slices(self):
        """Generates child slice orders with volume jitter preserving total quantity."""
        n = max(1, self.config.slices)
        base_qty = self.config.total_quantity / n
        interval = self.config.duration_seconds / n
        start_time = time.time()

        raw_quantities = []
        for _ in range(n):
            if self.config.jitter_pct > 0:
                factor = 1.0 + random.uniform(-self.config.jitter_pct, self.config.jitter_pct)
                raw_quantities.append(base_qty * factor)
            else:
                raw_quantities.append(base_qty)

        # Normalize to ensure sum equals exact total_quantity
        total_raw = sum(raw_quantities)
        if total_raw > 0:
            scale = self.config.total_quantity / total_raw
            quantities = [round(q * scale, 6) for q in raw_quantities]
            # Fix minor floating-point difference on final slice
            quantities[-1] += round(self.config.total_quantity - sum(quantities), 6)
        else:
            quantities = [base_qty] * n

        self.slices = [
            TWAPSlice(
                index=i,
                planned_qty=quantities[i],
                scheduled_time=start_time + (i * interval),
            )
            for i in range(n)
        ]

    async def execute_slice(self, slice_obj: TWAPSlice, current_market_price: float) -> ExecutionResult:
        """Executes a single child slice through the order executor."""
        # Check price limit constraint
        if self.config.price_limit is not None:
            if self.config.side == "BUY" and current_market_price > self.config.price_limit:
                slice_obj.status = "SKIPPED_PRICE_LIMIT"
                return ExecutionResult(
                    action_id=slice_obj.client_order_id,
                    success=False,
                    status_code=400,
                    data={"reason": "price_above_limit"},
                    latency_ms=0.0,
                    error=f"Current price {current_market_price} exceeds buy limit {self.config.price_limit}",
                )
            elif self.config.side == "SELL" and current_market_price < self.config.price_limit:
                slice_obj.status = "SKIPPED_PRICE_LIMIT"
                return ExecutionResult(
                    action_id=slice_obj.client_order_id,
                    success=False,
                    status_code=400,
                    data={"reason": "price_below_limit"},
                    latency_ms=0.0,
                    error=f"Current price {current_market_price} is below sell limit {self.config.price_limit}",
                )

        if self.arrival_price is None:
            self.arrival_price = current_market_price

        signal = Signal(
            source="twap_engine",
            target_id=self.config.symbol,
            action=self.config.side,
            payload={
                "symbol": self.config.symbol,
                "side": self.config.side,
                "quantity": slice_obj.planned_qty,
                "price": current_market_price if self.config.order_type == "LIMIT" else None,
                "type": self.config.order_type,
                "client_order_id": slice_obj.client_order_id,
                "idempotency_key": slice_obj.client_order_id,
                "slice_index": slice_obj.index,
            },
        )

        res = await self.order_executor.execute(signal)
        slice_obj.latency_ms = res.latency_ms

        if res.success:
            slice_obj.status = "FILLED"
            slice_obj.executed_qty = slice_obj.planned_qty
            slice_obj.fill_price = current_market_price
        else:
            slice_obj.status = "FAILED"

        return res

    def get_summary(self) -> Dict[str, Any]:
        """Calculates final execution metrics (average fill price, slippage bps, fill rate)."""
        filled_slices = [s for s in self.slices if s.status == "FILLED"]
        total_filled_qty = sum(s.executed_qty for s in filled_slices)
        
        avg_fill_price = 0.0
        if total_filled_qty > 0:
            avg_fill_price = sum(s.executed_qty * s.fill_price for s in filled_slices) / total_filled_qty

        # Slippage vs arrival price (in basis points)
        slippage_bps = 0.0
        if self.arrival_price and self.arrival_price > 0 and avg_fill_price > 0:
            if self.config.side == "BUY":
                slippage_bps = ((avg_fill_price - self.arrival_price) / self.arrival_price) * 10000.0
            else:
                slippage_bps = ((self.arrival_price - avg_fill_price) / self.arrival_price) * 10000.0

        return {
            "symbol": self.config.symbol,
            "side": self.config.side,
            "total_requested_qty": self.config.total_quantity,
            "total_filled_qty": round(total_filled_qty, 6),
            "fill_rate_pct": round((total_filled_qty / max(0.0001, self.config.total_quantity)) * 100.0, 2),
            "arrival_price": self.arrival_price,
            "average_fill_price": round(avg_fill_price, 4),
            "slippage_bps": round(slippage_bps, 2),
            "total_slices": len(self.slices),
            "filled_slices": len(filled_slices),
        }
