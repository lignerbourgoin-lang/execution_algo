from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    DROP = "drop"
    RESALE = "resale"
    MARKETPLACE = "marketplace"
    AFTER_QUEUE = "after_queue"


@dataclass
class Offer:
    id: str
    category: str
    price: float
    available: int
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class HoldResult:
    ok: bool
    qty: int = 0
    offer_id: str = ""
    checkout_url: str = ""
    raw: dict[str, Any] = field(default_factory=dict)
    error: str = ""


@dataclass
class Target:
    id: str
    type: EventType
    adapter: str
    quantity: int
    account_cap: int
    categories: list[str]
    urls: dict[str, str]
    session_headers: dict[str, str]
    session_cookies: dict[str, str]
    t0: float | None = None
    poll_min: float = 2.0
    poll_max: float = 4.0
    burst_shots: int = 5
    burst_spacing: float = 0.08
    max_price: float | None = None
    admit_timeout_s: float = 7200.0

    @property
    def buy_qty(self) -> int:
        return max(0, min(self.quantity, self.account_cap))
