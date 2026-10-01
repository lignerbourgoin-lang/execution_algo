from __future__ import annotations

import asyncio
import time
from typing import Any

from ..http import PersistentClient
from ..models import HoldResult, Offer, Target


class GenericJSONAdapter:
    def __init__(self, target: Target) -> None:
        self.target = target
        self.http = PersistentClient(target.session_headers, target.session_cookies)

    async def warmup(self) -> None:
        await self.http.warmup(self.target.urls.get("ping") or self.target.urls.get("stock"))

    async def read_cap(self) -> int | None:
        return None

    def _parse_offers(self, data: Any) -> list[Offer]:
        rows = data
        if isinstance(data, dict):
            rows = data.get("offers") or data.get("items") or data.get("tickets") or []
        out: list[Offer] = []
        if not isinstance(rows, list):
            return out
        for row in rows:
            if not isinstance(row, dict):
                continue
            oid = str(row.get("id") or row.get("offer_id") or "")
            if not oid:
                continue
            out.append(
                Offer(
                    id=oid,
                    category=str(row.get("category") or row.get("cat") or ""),
                    price=float(row.get("price") or 0),
                    available=int(row.get("available") or row.get("qty") or row.get("quantity") or 0),
                    raw=row,
                )
            )
        return out

    async def inventory(self, target: Target) -> list[Offer]:
        url = target.urls.get("stock")
        if not url:
            return []
        try:
            r = await self.http.get(url)
            r.raise_for_status()
            offers = self._parse_offers(r.json())
        except Exception:
            return []
        if target.categories:
            wanted = {c.upper() for c in target.categories}
            filtered = [o for o in offers if o.category.upper() in wanted]
            if filtered:
                order = {c.upper(): i for i, c in enumerate(target.categories)}
                filtered.sort(key=lambda o: order.get(o.category.upper(), 999))
                offers = filtered
        if target.max_price is not None:
            offers = [o for o in offers if o.price <= target.max_price]
        return [o for o in offers if o.available > 0]

    async def hold(self, target: Target, offer: Offer, qty: int) -> HoldResult:
        url = target.urls.get("hold")
        if not url:
            return HoldResult(ok=False, error="pas d'url hold")
        r = await self.http.post(url, json={"offer_id": offer.id, "qty": qty, "category": offer.category})
        try:
            body = r.json()
        except Exception:
            body = {"text": r.text}
        if r.status_code in (200, 201) and (body.get("ok", True) is not False):
            return HoldResult(
                ok=True,
                qty=int(body.get("qty") or qty),
                offer_id=offer.id,
                checkout_url=str(body.get("checkout_url") or body.get("url") or ""),
                raw=body if isinstance(body, dict) else {},
            )
        return HoldResult(ok=False, offer_id=offer.id, raw=body if isinstance(body, dict) else {}, error=str(r.status_code))

    async def checkout_url(self, hold: HoldResult) -> str:
        return hold.checkout_url

    async def wait_until_admitted(self, target: Target) -> None:
        url = target.urls.get("admitted")
        if not url:
            return
        deadline = time.time() + target.admit_timeout_s
        while time.time() < deadline:
            try:
                r = await self.http.get(url)
                data = r.json()
                if data.get("admitted") is True or data.get("status") == "admitted":
                    return
            except Exception:
                pass
            await asyncio.sleep(1.0)
        raise TimeoutError("admission timeout")

    async def aclose(self) -> None:
        await self.http.aclose()
