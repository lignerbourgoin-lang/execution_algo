"""
Official Resale Watcher
-----------------------
Watches an official resale listing feed (organizer exchange, authorized resale API, or any
JSON endpoint you are allowed to query) and alerts within one poll period when a ticket
matching your criteria appears. The purchase itself is validated by the human.

Pipeline: ConditionalPoller (304 / content hash) -> parse listings -> filter -> dedupe -> broadcast.
"""

import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Coroutine, Dict, List, Optional, Sequence

import httpx

from modules.retail.monitors.conditional_poll import ConditionalPoller
from modules.retail.notify.notifiers import Notification, Notifier, broadcast

logger = logging.getLogger("modules.retail.resale")

SEEN_LISTINGS_MAX = 100_000
NS_PER_MS = 1_000_000.0
DEFAULT_WATCH_POLL_INTERVAL_SEC = 5.0


@dataclass(frozen=True)
class ListingFieldPaths:
    """Dotted paths locating each field inside one listing object (e.g. "price.amount")."""

    listing_id: str = "id"
    price: str = "price"
    title: str = "title"
    url: str = "url"
    quantity: Optional[str] = "quantity"


@dataclass(frozen=True)
class ListingFilters:
    max_price: Optional[Decimal] = None
    min_quantity: int = 1
    title_keywords: Sequence[str] = ()  # all keywords must appear (case-insensitive)
    excluded_keywords: Sequence[str] = ()


@dataclass(frozen=True)
class Listing:
    listing_id: str
    title: str
    price: Optional[Decimal]
    quantity: Optional[int]
    url: Optional[str]


@dataclass
class ResaleWatchConfig:
    url: str
    items_path: str = ""  # dotted path to the listing array in the response ("" = root array)
    fields: ListingFieldPaths = field(default_factory=ListingFieldPaths)
    filters: ListingFilters = field(default_factory=ListingFilters)
    poll_interval_sec: float = DEFAULT_WATCH_POLL_INTERVAL_SEC
    headers: Dict[str, str] = field(default_factory=dict)
    alert_existing_on_start: bool = True


def extract_path(document: Any, dotted_path: str) -> Any:
    """Follows a dotted path through dicts and lists ("a.b.0.c"). Returns None if any step is missing."""
    if not dotted_path:
        return document
    current = document
    for key in dotted_path.split("."):
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and key.isdigit() and int(key) < len(current):
            current = current[int(key)]
        else:
            return None
        if current is None:
            return None
    return current


def _parse_price(raw_price: Any) -> Optional[Decimal]:
    if raw_price is None or isinstance(raw_price, bool):
        return None
    try:
        # str() first: Decimal(float) would carry binary noise (49.9 -> 49.899999...)
        return Decimal(str(raw_price).replace(",", "."))
    except InvalidOperation:
        return None


def _parse_quantity(raw_quantity: Any) -> Optional[int]:
    try:
        return int(raw_quantity) if raw_quantity is not None else None
    except (TypeError, ValueError):
        return None


def parse_listings(document: Any, items_path: str, fields: ListingFieldPaths) -> List[Listing]:
    """Turns a raw feed document into Listing objects. Items without an id are skipped and counted."""
    raw_items = extract_path(document, items_path)
    if not isinstance(raw_items, list):
        logger.warning("items_path '%s' did not resolve to a list (got %s)", items_path, type(raw_items).__name__)
        return []

    listings: List[Listing] = []
    skipped_without_id = 0
    for raw_item in raw_items:
        listing_id = extract_path(raw_item, fields.listing_id)
        if listing_id is None:
            skipped_without_id += 1
            continue
        listings.append(
            Listing(
                listing_id=str(listing_id),
                title=str(extract_path(raw_item, fields.title) or ""),
                price=_parse_price(extract_path(raw_item, fields.price)),
                quantity=_parse_quantity(extract_path(raw_item, fields.quantity)) if fields.quantity else None,
                url=extract_path(raw_item, fields.url),
            )
        )
    if skipped_without_id:
        logger.warning("%d listing(s) skipped: no value at id path '%s'", skipped_without_id, fields.listing_id)
    return listings


def matches_filters(listing: Listing, filters: ListingFilters) -> bool:
    # [FEATURE: RESALE_FILTER_FAIL_CLOSED] A listing with an unreadable price never passes a max_price filter.
    # Raison: a parsing problem must not page you for a 900 EUR ticket at 3am.
    # Attention: quantity is only enforced when the feed exposes it.
    if filters.max_price is not None and (listing.price is None or listing.price > filters.max_price):
        return False
    if listing.quantity is not None and listing.quantity < filters.min_quantity:
        return False
    title_lower = listing.title.lower()
    if any(keyword.lower() not in title_lower for keyword in filters.title_keywords):
        return False
    if any(keyword.lower() in title_lower for keyword in filters.excluded_keywords):
        return False
    return True


class ResaleWatcher:
    def __init__(
        self,
        config: ResaleWatchConfig,
        notifiers: Sequence[Notifier],
        transport: Optional[httpx.AsyncBaseTransport] = None,
        auto_reserve_action: Optional[Callable[[Listing], Coroutine[Any, Any, Any]]] = None,
    ):
        if not notifiers:
            raise ValueError("At least one notifier is required (an unseen alert is a no-op)")
        self.config = config
        self.notifiers = list(notifiers)
        self.auto_reserve_action = auto_reserve_action
        self._seen_listing_ids: "OrderedDict[str, None]" = OrderedDict()
        self._is_first_batch = True
        self.alerts_sent = 0
        self.last_alert_latency_ms: Optional[float] = None
        self.poller = ConditionalPoller(
            url=config.url,
            poll_interval_sec=config.poll_interval_sec,
            headers=config.headers,
            emit_initial=True,
            transport=transport,
        )
        self.poller.on_change(self.handle_document)

    def _remember(self, listing_id: str) -> None:
        self._seen_listing_ids[listing_id] = None
        if len(self._seen_listing_ids) > SEEN_LISTINGS_MAX:
            self._seen_listing_ids.popitem(last=False)

    def select_new_matches(self, document: Any) -> List[Listing]:
        """Returns listings never seen before that pass the filters, and marks every listing as seen."""
        new_matches: List[Listing] = []
        is_first_batch = self._is_first_batch
        self._is_first_batch = False
        for listing in parse_listings(document, self.config.items_path, self.config.fields):
            if listing.listing_id in self._seen_listing_ids:
                continue
            self._remember(listing.listing_id)
            if is_first_batch and not self.config.alert_existing_on_start:
                continue
            if matches_filters(listing, self.config.filters):
                new_matches.append(listing)
        return new_matches

    async def handle_document(self, document: Any, received_ns: int) -> None:
        for listing in self.select_new_matches(document):
            checkout_url = listing.url
            reserved_cart = None
            if self.auto_reserve_action:
                try:
                    reserved_cart = await self.auto_reserve_action(listing)
                    if reserved_cart and hasattr(reserved_cart, "checkout_url"):
                        checkout_url = reserved_cart.checkout_url
                    elif isinstance(reserved_cart, dict) and "checkout_url" in reserved_cart:
                        checkout_url = reserved_cart["checkout_url"]
                except Exception as e:
                    logger.warning("Auto-reserve for listing %s failed: %r", listing.listing_id, e)

            price_text = f"{listing.price} EUR" if listing.price is not None else "prix inconnu"
            quantity_text = f" x{listing.quantity}" if listing.quantity is not None else ""
            title_prefix = "🎟️ [PANIER VERROUILLÉ] " if reserved_cart else "Revente : "
            notification = Notification(
                title=f"{title_prefix}{listing.title or listing.listing_id}",
                message=f"{price_text}{quantity_text}" + (" (Réservé au panier !)" if reserved_cart else ""),
                url=checkout_url,
                is_urgent=True,
            )
            await broadcast(self.notifiers, notification)
            self.alerts_sent += 1
            self.last_alert_latency_ms = (time.perf_counter_ns() - received_ns) / NS_PER_MS
            logger.info("Alert for %s sent %.2f ms after reception", listing.listing_id, self.last_alert_latency_ms)

    async def start(self) -> None:
        await self.poller.start()

    async def stop(self) -> None:
        await self.poller.stop()
