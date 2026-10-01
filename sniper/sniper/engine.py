from __future__ import annotations

import asyncio
import random

from .adapters.generic_json import GenericJSONAdapter
from .adapters.shopify_generic import ShopifyGenericAdapter
from .clock import Clock
from .models import EventType, HoldResult, Offer, Target
from .notify import Notify
from .system import freeze_gc, win_timer_1ms


def make_adapter(target: Target):
    if target.adapter == "generic_json":
        return GenericJSONAdapter(target)
    if target.adapter == "shopify_generic":
        return ShopifyGenericAdapter(target)
    raise ValueError(f"adapter inconnu: {target.adapter}")


class Engine:
    def __init__(self, target: Target, notify: Notify, clock: Clock | None = None) -> None:
        self.target = target
        self.notify = notify
        self.clock = clock or Clock()
        self.adapter = make_adapter(target)

    async def run(self, dry_run: bool = False) -> HoldResult | None:
        cap = await self.adapter.read_cap()
        if cap is not None:
            self.target.account_cap = cap
        qty = self.target.buy_qty
        await self.notify.urgent(
            f"[{self.target.id}] quantity={self.target.quantity} cap={self.target.account_cap} buy_qty={qty}"
        )
        if qty <= 0:
            await self.notify.urgent("buy_qty=0, stop")
            return None

        await self.adapter.warmup()
        try:
            if dry_run:
                offers = await self.adapter.inventory(self.target)
                await self.notify.urgent(f"dry-run offers={len(offers)} buy_qty={qty}")
                return None
            if self.target.type is EventType.DROP:
                return await self._drop()
            if self.target.type is EventType.RESALE:
                return await self._watch()
            if self.target.type is EventType.MARKETPLACE:
                return await self._watch()
            if self.target.type is EventType.AFTER_QUEUE:
                return await self._after_queue()
            raise ValueError(self.target.type)
        finally:
            await self.adapter.aclose()

    def _wanted_qty(self, offer: Offer) -> int:
        return max(0, min(self.target.buy_qty, offer.available))

    async def _try_offers(self, offers: list[Offer]) -> HoldResult | None:
        for offer in offers:
            qty = self._wanted_qty(offer)
            if qty <= 0:
                continue
            hold = await self.adapter.hold(self.target, offer, qty)
            if hold.ok:
                url = await self.adapter.checkout_url(hold)
                await self.notify.urgent(
                    f"LOCK {self.target.id} qty={hold.qty} cat={offer.category} {url}"
                )
                return hold
        return None

    async def _shot(self) -> HoldResult | None:
        offers = await self.adapter.inventory(self.target)
        return await self._try_offers(offers)

    async def _drop(self) -> HoldResult | None:
        if self.target.t0 is None:
            raise ValueError("t0 manquant")
        delays = [i * self.target.burst_spacing for i in range(self.target.burst_shots)]
        self.clock.sleep_until(self.target.t0)
        with win_timer_1ms(), freeze_gc():
            tasks = [asyncio.create_task(self._delayed_shot(d)) for d in delays]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for p in pending:
                p.cancel()
            for d in done:
                res = d.result()
                if res and res.ok:
                    return res
            return None

    async def _delayed_shot(self, delay: float) -> HoldResult | None:
        if delay:
            await asyncio.sleep(delay)
        return await self._shot()

    async def _watch(self) -> HoldResult | None:
        while True:
            await asyncio.sleep(random.uniform(self.target.poll_min, self.target.poll_max))
            hit = await self._shot()
            if hit:
                return hit

    async def _after_queue(self) -> HoldResult | None:
        await self.adapter.wait_until_admitted(self.target)
        return await self._shot()
