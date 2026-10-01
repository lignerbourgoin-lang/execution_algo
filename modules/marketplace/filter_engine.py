"""
Marketplace Structured Filter Engine
------------------------------------
High-speed evaluation pipeline for second-hand marketplaces (Vinted, Leboncoin, eBay, etc.).
Filters incoming item listings against strict price, keyword, seller trust, and condition rules.
"""

from dataclasses import dataclass, field
import logging
import re
import time
from typing import Any, Dict, List, Optional, Set

from core.engine.base import BaseStrategy, Signal

logger = logging.getLogger("execution.marketplace.filter")


@dataclass
class FilterRule:
    min_price: float = 0.0
    max_price: float = float("inf")
    include_keywords: List[str] = field(default_factory=list)  # Any matching keyword qualifies
    must_include_all: List[str] = field(default_factory=list)  # All must be present
    exclude_keywords: List[str] = field(default_factory=list)  # Any matching keyword disqualifies
    min_seller_rating: float = 0.0                             # e.g. 4.5 / 5.0
    min_seller_reviews: int = 0                               # e.g. at least 5 reviews
    allowed_countries: Set[str] = field(default_factory=set)   # e.g. {"FR", "BE", "ES"}
    max_item_age_sec: float = 300.0                           # Ignore items published > 5 mins ago


class MarketplaceFilterStrategy(BaseStrategy):
    """
    Evaluates incoming marketplace listings against FilterRule specifications.
    Emits an urgent BUY_ITEM signal if all criteria are satisfied.
    """

    def __init__(self, target_marketplace: str, rule: FilterRule):
        self.target_marketplace = target_marketplace
        self.rule = rule
        self._compile_patterns()

    def _compile_patterns(self):
        """Pre-compiles regex patterns for zero-overhead string search."""
        self._exclude_re = (
            re.compile(r"\b(" + "|".join(re.escape(k) for k in self.rule.exclude_keywords) + r")\b", re.IGNORECASE)
            if self.rule.exclude_keywords
            else None
        )
        self._include_re = (
            re.compile(r"\b(" + "|".join(re.escape(k) for k in self.rule.include_keywords) + r")\b", re.IGNORECASE)
            if self.rule.include_keywords
            else None
        )
        self._must_re = [
            re.compile(r"\b" + re.escape(k) + r"\b", re.IGNORECASE)
            for k in self.rule.must_include_all
        ]

    def evaluate(self, market_data: Dict[str, Any]) -> Optional[Signal]:
        """
        Evaluates an individual item listing.
        Returns Signal if the item matches all filter criteria.
        """
        # 1. Price Check
        price = float(market_data.get("price", 0.0))
        if price < self.rule.min_price or price > self.rule.max_price:
            return None

        # 2. Text Search (Title + Description)
        title = market_data.get("title", "")
        description = market_data.get("description", "")
        full_text = f"{title} {description}".lower()

        # Check Exclude Keywords (blacklist)
        if self._exclude_re and self._exclude_re.search(full_text):
            return None

        # Check Must Include All
        for req_pattern in self._must_re:
            if not req_pattern.search(full_text):
                return None

        # Check Include Keywords (at least one match if list is non-empty)
        if self._include_re and not self._include_re.search(full_text):
            return None

        # 3. Seller Trust Evaluation
        seller = market_data.get("seller", {})
        if isinstance(seller, dict):
            rating = float(seller.get("rating", 5.0))
            review_count = int(seller.get("review_count", 0))

            if rating < self.rule.min_seller_rating:
                return None
            if review_count < self.rule.min_seller_reviews:
                return None

            country = seller.get("country", "").upper()
            if self.rule.allowed_countries and country not in self.rule.allowed_countries:
                return None

        # 4. Item Freshness Check
        created_at = market_data.get("created_at_epoch")
        if created_at:
            age_sec = time.time() - float(created_at)
            if age_sec > self.rule.max_item_age_sec:
                return None

        # All criteria passed -> Generate immediate action signal
        item_id = str(market_data.get("id") or market_data.get("item_id", "unknown"))
        return Signal(
            source=f"marketplace_{self.target_marketplace}",
            target_id=item_id,
            action="BUY_ITEM",
            payload={
                "marketplace": self.target_marketplace,
                "item_id": item_id,
                "price": price,
                "title": title,
                "seller_id": seller.get("id") if isinstance(seller, dict) else None,
                "checkout_url": market_data.get("checkout_url"),
            },
            urgency=3,  # Immediate / highest priority
        )
