"""
Finance Execution Modules
-------------------------
Algorithmic order execution and market microstructure strategies:
- TWAP (Time-Weighted Average Price)
- VWAP (Volume-Weighted Average Price)
- Spatial Cross-Venue Arbitrage
- Financial Exchange Order Router
"""

from modules.finance.twap import TWAPConfig, TWAPExecutor, TWAPSlice, TWAPStrategy
from modules.finance.vwap import VWAPConfig, VWAPExecutor, VWAPSlice, VWAPStrategy
from modules.finance.arbitrage import (
    ArbitrageOpportunity,
    SpatialArbitrageStrategy,
    VenueQuote,
)
from modules.finance.order_router import ExchangeCredentials, ExchangeOrderExecutor

__all__ = [
    "TWAPConfig",
    "TWAPExecutor",
    "TWAPSlice",
    "TWAPStrategy",
    "VWAPConfig",
    "VWAPExecutor",
    "VWAPSlice",
    "VWAPStrategy",
    "ArbitrageOpportunity",
    "SpatialArbitrageStrategy",
    "VenueQuote",
    "ExchangeCredentials",
    "ExchangeOrderExecutor",
]
