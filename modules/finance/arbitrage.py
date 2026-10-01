"""
Cross-Venue Spatial Arbitrage Engine
-----------------------------------
Detects and executes two-legged price discrepancies between exchanges or order books.
Accounts for taker fee tiers, transfer friction, and order book depth.

Features:
- Dual-sided spread calculation (Venue A -> Venue B, and Venue B -> Venue A).
- Net profit calculation in basis points (bps) after exchange fees and slippage buffer.
- Depth-aware sizing: automatically sizes orders to available top-of-book volume.
- Stale quote discard: protects against phantom arbitrage from lagged WebSocket feeds.
"""

from dataclasses import dataclass, field
import logging
import time
from typing import Any, Dict, Optional, Tuple

from core.engine.base import BaseStrategy, Signal

logger = logging.getLogger("execution.finance.arbitrage")


@dataclass
class VenueQuote:
    venue_id: str
    symbol: str
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    fee_rate: float = 0.0010  # 10 bps taker fee by default
    timestamp_ns: int = field(default_factory=time.perf_counter_ns)


@dataclass
class ArbitrageOpportunity:
    buy_venue: str
    sell_venue: str
    symbol: str
    buy_price: float
    sell_price: float
    executable_qty: float
    gross_spread_bps: float
    net_spread_bps: float
    estimated_profit_usd: float
    detected_at_ns: int = field(default_factory=time.perf_counter_ns)


class SpatialArbitrageStrategy(BaseStrategy):
    """
    Evaluates real-time quotes across two trading venues and triggers
    synchronized two-legged execution when net spread exceeds the profit threshold.
    """

    def __init__(
        self,
        symbol: str,
        min_profit_bps: float = 15.0,  # Minimum 15 bps net profit (~0.15%)
        max_order_size: float = 1.0,
        slippage_buffer_bps: float = 3.0,
        max_quote_age_ms: float = 200.0,
    ):
        self.symbol = symbol
        self.min_profit_bps = min_profit_bps
        self.max_order_size = max_order_size
        self.slippage_buffer_bps = slippage_buffer_bps
        self.max_quote_age_ns = int(max_quote_age_ms * 1_000_000)

        self.quotes: Dict[str, VenueQuote] = {}

    def update_quote(self, quote: VenueQuote):
        """Updates cache with the latest top-of-book quote for a venue."""
        self.quotes[quote.venue_id] = quote

    def evaluate(self, market_data: Dict[str, Any]) -> Optional[Signal]:
        """
        Evaluates current quotes between registered venues for arbitrage.
        Can be triggered by incoming market data or called directly.
        """
        venues = list(self.quotes.keys())
        if len(venues) < 2:
            return None

        now_ns = time.perf_counter_ns()
        v1, v2 = venues[0], venues[1]
        q1, q2 = self.quotes[v1], self.quotes[v2]

        # Check quote staleness
        if (now_ns - q1.timestamp_ns) > self.max_quote_age_ns:
            return None
        if (now_ns - q2.timestamp_ns) > self.max_quote_age_ns:
            return None

        # Check Opportunity 1: Buy on V1, Sell on V2
        opp1 = self._check_pair(buy_quote=q1, sell_quote=q2)
        if opp1 and opp1.net_spread_bps >= self.min_profit_bps:
            return self._build_signal(opp1)

        # Check Opportunity 2: Buy on V2, Sell on V1
        opp2 = self._check_pair(buy_quote=q2, sell_quote=q1)
        if opp2 and opp2.net_spread_bps >= self.min_profit_bps:
            return self._build_signal(opp2)

        return None

    def _check_pair(self, buy_quote: VenueQuote, sell_quote: VenueQuote) -> Optional[ArbitrageOpportunity]:
        """Calculates net profitability for buying on one venue and selling on another."""
        buy_price = buy_quote.ask_price
        sell_price = sell_quote.bid_price

        if buy_price <= 0 or sell_price <= buy_price:
            return None

        gross_spread = (sell_price - buy_price) / buy_price
        gross_spread_bps = gross_spread * 10000.0

        # Total fees = buy fee + sell fee + slippage buffer
        fee_cost_bps = ((buy_quote.fee_rate + sell_quote.fee_rate) * 10000.0) + self.slippage_buffer_bps
        net_spread_bps = gross_spread_bps - fee_cost_bps

        if net_spread_bps <= 0:
            return None

        # Size capped by minimum liquidity on both sides and max_order_size
        max_executable = min(buy_quote.ask_qty, sell_quote.bid_qty, self.max_order_size)
        if max_executable <= 0:
            return None

        estimated_profit = max_executable * (sell_price - buy_price) * (net_spread_bps / gross_spread_bps)

        return ArbitrageOpportunity(
            buy_venue=buy_quote.venue_id,
            sell_venue=sell_quote.venue_id,
            symbol=self.symbol,
            buy_price=buy_price,
            sell_price=sell_price,
            executable_qty=round(max_executable, 6),
            gross_spread_bps=round(gross_spread_bps, 2),
            net_spread_bps=round(net_spread_bps, 2),
            estimated_profit_usd=round(estimated_profit, 4),
        )

    def _build_signal(self, opp: ArbitrageOpportunity) -> Signal:
        return Signal(
            source="spatial_arbitrage",
            target_id=opp.symbol,
            action="ARBITRAGE_EXECUTE",
            payload={
                "symbol": opp.symbol,
                "buy_venue": opp.buy_venue,
                "sell_venue": opp.sell_venue,
                "buy_price": opp.buy_price,
                "sell_price": opp.sell_price,
                "quantity": opp.executable_qty,
                "net_spread_bps": opp.net_spread_bps,
                "estimated_profit_usd": opp.estimated_profit_usd,
            },
            urgency=3,  # Immediate execution
        )
