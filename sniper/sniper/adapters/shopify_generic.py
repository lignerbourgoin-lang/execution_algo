from __future__ import annotations

from typing import Any

from ..models import HoldResult, Offer, Target
from .generic_json import GenericJSONAdapter


class ShopifyGenericAdapter(GenericJSONAdapter):
    """
    Shopify Cart & Checkout Adapter (sans bypass de queue).
    Interagit avec l'API /cart/add.js et /checkout une fois la session admise.
    """

    async def hold(self, target: Target, offer: Offer, qty: int) -> HoldResult:
        base_url = target.urls.get("hold") or target.urls.get("stock", "").rstrip("/")
        add_url = f"{base_url}/cart/add.js" if not base_url.endswith("/cart/add.js") else base_url
        try:
            r = await self.http.post(add_url, json={"id": offer.id, "quantity": qty})
            if r.status_code in (200, 201):
                checkout_url = f"{base_url.split('/cart')[0]}/checkout"
                return HoldResult(
                    ok=True,
                    qty=qty,
                    offer_id=offer.id,
                    checkout_url=checkout_url,
                    raw=r.json() if r.headers.get("content-type", "").startswith("application/json") else {},
                )
            return HoldResult(ok=False, offer_id=offer.id, error=f"HTTP {r.status_code}")
        except Exception as e:
            return HoldResult(ok=False, offer_id=offer.id, error=str(e))
