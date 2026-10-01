"""
VWAP (Volume-Weighted Average Price) Execution Algorithm
--------------------------------------------------------
Slices parent orders proportionally to expected or real-time volume profiles,
minimizing market impact and tracking the volume-weighted benchmark price.

Features:
- Dynamic or profile-based volume weighting (e.g., U-shaped intraday curve).
- Participation rate guard (e.g., max 10% of interval volume) to prevent price impact.
- Real-time VWAP benchmark tracking (Market VWAP vs Execution VWAP).
- Performance calculation in basis points (bps) outperformance/underperformance.
"""

from dataclasses import dataclass, field
import logging
import time
from typing import Any, Dict, List, Optional
import uuid

from core.engine.base import BaseExecutor, BaseStrategy, ExecutionResult, Signal

logger = logging.getLogger("execution.finance.vwap")


@dataclass
class VWAPConfig:
    symbol: str
    side: str  # "BUY" or "SELL"
    total_quantity: float
    duration_seconds: float
    slices: int
    volume_profile: Optional[List[float]] = None  # Expected volume distribution weights
    max_participation_rate: float = 0.15  # Max 15% of interval volume
    price_limit: Optional[float] = None
    order_type: str = "LIMIT"


@dataclass
class VWAPSlice:
    index: int
    planned_qty: float
    weight: float
    executed_qty: float = 0.0
    fill_price: float = 0.0
    status: str = "PENDING"
    client_order_id: str = field(default_factory=lambda: f"vwap_{uuid.uuid4().hex[:12]}")
    latency_ms: float = 0.0


class VWAPStrategy(BaseStrategy):
    """
    Evaluates incoming order book and trade prints to calculate
    market VWAP and monitor volume pace.
    """

    def __init__(self, config: VWAPConfig):
        self.config = config
        self.cumulative_market_volume: float = 0.0
        self.cumulative_market_notional: float = 0.0
        self.market_vwap: float = 0.0

    def evaluate(self, market_data: Dict[str, Any]) -> Optional[Signal]:
        price = market_data.get("price") or market_data.get("close")
        volume = market_data.get("volume") or market_data.get("qty", 0.0)

        if price is not None and volume > 0:
            price = float(price)
            volume = float(volume)
            self.cumulative_market_volume += volume
            self.cumulative_market_notional += price * volume
            if self.cumulative_market_volume > 0:
                self.market_vwap = self.cumulative_market_notional / self.cumulative_market_volume

        return None


class VWAPExecutor:
    """
    Coordinates slice execution according to volume curve weights and participation limits.
    """

    def __init__(self, config: VWAPConfig, order_executor: BaseExecutor):
        self.config = config
        self.order_executor = order_executor
        self.slices: List[VWAPSlice] = []
        self.cumulative_market_vol: float = 0.0
        self.cumulative_market_notional: float = 0.0
        self._initialize_slices()

    def _initialize_slices(self):
        """Generates slices weighted by the volume profile (e.g. U-shaped or custom)."""
        n = max(1, self.config.slices)

        if self.config.volume_profile and len(self.config.volume_profile) == n:
            profile = self.config.volume_profile
        else:
            # Default U-shaped volume curve typical of equity / crypto trading sessions
            # Higher volume at open/close, lower volume midday
            mid = (n - 1) / 2.0
            profile = [1.0 + ((i - mid) / max(1.0, mid)) ** 2 for i in range(n)]

        total_weight = sum(profile)
        weights = [w / total_weight for w in profile]

        quantities = [round(self.config.total_quantity * w, 6) for w in weights]
        # Adjust rounding difference on final slice
        quantities[-1] += round(self.config.total_quantity - sum(quantities), 6)

        self.slices = [
            VWAPSlice(
                index=i,
                planned_qty=quantities[i],
                weight=weights[i],
            )
            for i in range(n)
        ]

    def record_market_trade(self, price: float, volume: float):
        """Tracks the benchmark market VWAP from public trades."""
        if volume > 0 and price > 0:
            self.cumulative_market_vol += volume
            self.cumulative_market_notional += price * volume

    @property
    def market_vwap(self) -> float:
        if self.cumulative_market_vol > 0:
            return self.cumulative_market_notional / self.cumulative_market_vol
        return 0.0

    async def execute_slice(
        self,
        slice_obj: VWAPSlice,
        current_market_price: float,
        interval_market_volume: Optional[float] = None,
    ) -> ExecutionResult:
        """Executes a VWAP slice, respecting price limits and volume participation caps."""
        # 1. Price Limit Guard
        if self.config.price_limit is not None:
            if self.config.side == "BUY" and current_market_price > self.config.price_limit:
                slice_obj.status = "SKIPPED_PRICE_LIMIT"
                return ExecutionResult(
                    action_id=slice_obj.client_order_id,
                    success=False,
                    status_code=400,
                    data={"reason": "price_above_limit"},
                    latency_ms=0.0,
                    error=f"Price {current_market_price} exceeds limit {self.config.price_limit}",
                )
            elif self.config.side == "SELL" and current_market_price < self.config.price_limit:
                slice_obj.status = "SKIPPED_PRICE_LIMIT"
                return ExecutionResult(
                    action_id=slice_obj.client_order_id,
                    success=False,
                    status_code=400,
                    data={"reason": "price_below_limit"},
                    latency_ms=0.0,
                    error=f"Price {current_market_price} is below limit {self.config.price_limit}",
                )

        # 2. Participation Rate Cap: do not exceed max_participation_rate of interval volume
        exec_qty = slice_obj.planned_qty
        if interval_market_volume and interval_market_volume > 0:
            max_allowed = interval_market_volume * self.config.max_participation_rate
            if exec_qty > max_allowed:
                exec_qty = round(max(0.0001, max_allowed), 6)

        signal = Signal(
            source="vwap_engine",
            target_id=self.config.symbol,
            action=self.config.side,
            payload={
                "symbol": self.config.symbol,
                "side": self.config.side,
                "quantity": exec_qty,
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
            slice_obj.executed_qty = exec_qty
            slice_obj.fill_price = current_market_price
        else:
            slice_obj.status = "FAILED"

        return res

    def get_summary(self) -> Dict[str, Any]:
        """Calculates execution VWAP vs market VWAP and outperformance in bps."""
        filled_slices = [s for s in self.slices if s.status == "FILLED"]
        total_filled_qty = sum(s.executed_qty for s in filled_slices)

        exec_vwap = 0.0
        if total_filled_qty > 0:
            exec_vwap = sum(s.executed_qty * s.fill_price for s in filled_slices) / total_filled_qty

        # Benchmark comparison
        mkt_vwap = self.market_vwap
        outperformance_bps = 0.0
        if mkt_vwap > 0 and exec_vwap > 0:
            if self.config.side == "BUY":
                # For BUY, lower exec price than market VWAP = positive outperformance
                outperformance_bps = ((mkt_vwap - exec_vwap) / mkt_vwap) * 10000.0
            else:
                # For SELL, higher exec price than market VWAP = positive outperformance
                outperformance_bps = ((exec_vwap - mkt_vwap) / mkt_vwap) * 10000.0

        return {
            "symbol": self.config.symbol,
            "side": self.config.side,
            "total_requested_qty": self.config.total_quantity,
            "total_filled_qty": round(total_filled_qty, 6),
            "execution_vwap": round(exec_vwap, 4),
            "market_vwap": round(mkt_vwap, 4),
            "outperformance_bps": round(outperformance_bps, 2),
            "total_slices": len(self.slices),
            "filled_slices": len(filled_slices),
        }
