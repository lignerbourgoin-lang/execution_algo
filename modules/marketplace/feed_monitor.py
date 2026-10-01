"""
Marketplace Feed Monitor & Item Detector
----------------------------------------
Polls marketplace search endpoints or processes websocket/webhook feeds,
deduplicating listings and measuring end-to-end detection latency.

Features:
- Bounded LRU deduplication cache (prevents duplicate processing & unbounded RAM growth).
- Timestamp diff tracking (publication time vs detector arrival time).
- Asynchronous callback pipeline.
"""

from collections import OrderedDict
import logging
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional

logger = logging.getLogger("execution.marketplace.feed")


class MarketplaceFeedMonitor:
    """
    Monitors listing feeds, removes duplicates, and alerts downstream strategies.
    """

    def __init__(
        self,
        marketplace_name: str,
        max_seen_cache: int = 10000,
    ):
        self.marketplace_name = marketplace_name
        self.max_seen_cache = max_seen_cache
        self._seen_ids: OrderedDict[str, float] = OrderedDict()
        self._callbacks: List[Callable[[Dict[str, Any], int], Coroutine[Any, Any, None]]] = []
        self.total_detected: int = 0
        self.duplicates_ignored: int = 0

    def register_callback(self, callback: Callable[[Dict[str, Any], int], Coroutine[Any, Any, None]]):
        """Registers an async callback invoked on every newly discovered item."""
        self._callbacks.append(callback)

    def is_new_item(self, item_id: str) -> bool:
        """Checks if item has been seen before; marks as seen if new."""
        if item_id in self._seen_ids:
            self.duplicates_ignored += 1
            return False

        # Add to seen cache with eviction if capacity exceeded
        self._seen_ids[item_id] = time.time()
        if len(self._seen_ids) > self.max_seen_cache:
            self._seen_ids.popitem(last=False)  # FIFO eviction

        return True

    async def ingest_items(self, raw_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes a batch of items from search or feed.
        Returns the list of newly discovered items and dispatches callbacks.
        """
        now_ns = time.perf_counter_ns()
        new_items = []

        for item in raw_items:
            item_id = str(item.get("id") or item.get("item_id", ""))
            if not item_id:
                continue

            if self.is_new_item(item_id):
                self.total_detected += 1
                new_items.append(item)

                # Measure detection latency if publication timestamp is available
                pub_epoch = item.get("created_at_epoch")
                if pub_epoch:
                    detection_latency_sec = max(0.0, time.time() - float(pub_epoch))
                    item["detection_latency_sec"] = round(detection_latency_sec, 3)

                # Dispatch to registered callbacks
                for cb in self._callbacks:
                    try:
                        await cb(item, now_ns)
                    except Exception as e:
                        logger.error(f"Error in marketplace feed callback: {e}")

        return new_items

    def get_stats(self) -> Dict[str, Any]:
        return {
            "marketplace": self.marketplace_name,
            "total_detected": self.total_detected,
            "duplicates_ignored": self.duplicates_ignored,
            "cached_seen_count": len(self._seen_ids),
        }
