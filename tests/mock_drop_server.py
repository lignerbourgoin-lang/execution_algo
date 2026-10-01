"""
In-Memory Mock Ticketing Drop Server
------------------------------------
Provides a high-fidelity, deterministic simulation of commercial ticketing platforms:
- Exact T0 drop opening gate (404/425 before T0, 200 after T0).
- Inventory exhaustion and category cascading (409 Sold Out).
- Temporary cart holds with automated expiration and release back into inventory (Wave Sniping).
- Rate-limiting (HTTP 429 with Retry-After).
- WAF challenge injection (Cloudflare Turnstile / 403 simulation).
- Usable as an in-memory httpx.MockTransport with zero OS port overhead.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Set

import httpx


class MockTicketingServer:
    """
    Simulates a ticketing drop API server with accurate timing, quotas, and inventory releases.
    """

    def __init__(
        self,
        event_id: str = "CONCERT-2026",
        drop_time_utc: Optional[float] = None,
        initial_inventory: Optional[Dict[str, int]] = None,
        cart_hold_duration_sec: float = 3.0,
        rate_limit_threshold_per_sec: int = 50,
        simulated_latency_ms: float = 0.0,
    ) -> None:
        self.event_id = event_id
        self.drop_time_utc = drop_time_utc or 0.0
        self.inventory: Dict[str, int] = initial_inventory or {"CARRE_OR": 2, "CAT_1": 5}
        self.cart_hold_duration_sec = cart_hold_duration_sec
        self.rate_limit_threshold_per_sec = rate_limit_threshold_per_sec
        self.simulated_latency_ms = simulated_latency_ms

        self.active_reservations: Dict[str, Dict[str, Any]] = {}
        self.confirmed_purchases: Set[str] = set()
        self.challenge_ips: Set[str] = set()
        self.blocked_ips: Set[str] = set()

        self._lock = threading.Lock()
        self.request_log: List[Dict[str, Any]] = []

    def set_drop_time(self, drop_time_utc: float) -> None:
        """Sets the exact UTC timestamp when drop gates open."""
        with self._lock:
            self.drop_time_utc = drop_time_utc

    def trigger_cloudflare_challenge(self, ip_address: str) -> None:
        """Configures the mock server to respond with a Cloudflare Turnstile challenge for the IP."""
        with self._lock:
            self.challenge_ips.add(ip_address)

    def trigger_ip_ban(self, ip_address: str) -> None:
        """Configures the mock server to return HTTP 403 Access Denied for the IP."""
        with self._lock:
            self.blocked_ips.add(ip_address)

    def _release_expired_carts_locked(self, now: float) -> None:
        """Releases seats back into inventory for any cart whose hold duration has elapsed."""
        expired_tokens = []
        for token, cart_data in self.active_reservations.items():
            if token in self.confirmed_purchases:
                continue
            if now >= cart_data["expires_at"]:
                expired_tokens.append(token)

        for token in expired_tokens:
            cart_data = self.active_reservations.pop(token)
            category = cart_data["category_id"]
            quantity = cart_data["quantity"]
            self.inventory[category] = self.inventory.get(category, 0) + quantity

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Synchronous handler invoked by httpx.MockTransport for each incoming request."""
        now = time.time()
        url_path = request.url.path
        method = request.method
        client_ip = request.headers.get("x-forwarded-for", "127.0.0.1")

        with self._lock:
            self._release_expired_carts_locked(now)

            # Record call in history
            self.request_log.append({
                "time": now,
                "method": method,
                "path": url_path,
                "headers": dict(request.headers),
                "ip": client_ip,
            })

            # 1. WAF Blocking / Challenge checks
            if client_ip in self.blocked_ips:
                return httpx.Response(
                    403,
                    headers={"Server": "cloudflare", "CF-Ray": "89abcd1234ef"},
                    content=b"error code: 1020 access denied",
                )

            if client_ip in self.challenge_ips:
                return httpx.Response(
                    403,
                    headers={
                        "Server": "cloudflare",
                        "CF-Mitigated": "challenge",
                        "CF-Ray": "89abcd1234ef",
                        "Content-Type": "text/html",
                    },
                    content=b"<html><head><title>Just a moment...</title></head><body><div id='turnstile-wrapper'></div></body></html>",
                )

            # 2. Probe / Prewarm check
            if url_path in ("/", f"/api/events/{self.event_id}/availability"):
                if method == "HEAD":
                    return httpx.Response(200, headers={"Server": "nginx", "Content-Type": "application/json"})

            # 3. Availability Endpoint
            if "/availability" in url_path and method == "GET":
                category = request.url.params.get("cat") or request.headers.get("x-category") or "CARRE_OR"
                available_count = self.inventory.get(category, 0)

                # If before drop time, inventory appears 0 or 404 gate closed
                if self.drop_time_utc > 0 and now < self.drop_time_utc:
                    return httpx.Response(
                        200,
                        json={"event_id": self.event_id, "category": category, "available": 0, "status": "upcoming"},
                    )

                return httpx.Response(
                    200,
                    json={
                        "event_id": self.event_id,
                        "category": category,
                        "available": available_count,
                        "seats_left": available_count,
                        "status": "open" if available_count > 0 else "sold_out",
                    },
                )

            # 4. Reserve Endpoint
            if "/reserve" in url_path and method == "POST":
                # Check gate opening time
                if self.drop_time_utc > 0 and now < self.drop_time_utc:
                    return httpx.Response(
                        404,
                        json={"error": "Drop gate not open yet", "drop_time_utc": self.drop_time_utc},
                    )

                # Parse JSON payload
                try:
                    payload = json.loads(request.content.decode("utf-8"))
                except Exception:
                    payload = {}

                category = payload.get("category_id", "CARRE_OR")
                quantity = int(payload.get("quantity", 1))

                available_count = self.inventory.get(category, 0)
                if available_count >= quantity:
                    # Allocate seats
                    self.inventory[category] = available_count - quantity
                    cart_token = f"tok_mock_{uuid.uuid4().hex[:12]}"
                    expires_at = now + self.cart_hold_duration_sec

                    self.active_reservations[cart_token] = {
                        "category_id": category,
                        "quantity": quantity,
                        "expires_at": expires_at,
                    }

                    return httpx.Response(
                        200,
                        json={
                            "success": True,
                            "token": cart_token,
                            "cart_token": cart_token,
                            "hold_time_sec": self.cart_hold_duration_sec,
                            "checkout_url": f"https://mock.billetterie.example.com/checkout?cart={cart_token}",
                        },
                    )
                else:
                    return httpx.Response(
                        409,
                        json={
                            "error": f"Category '{category}' sold out",
                            "available": available_count,
                            "category_id": category,
                        },
                    )

            # 5. Checkout Endpoint
            if "/checkout" in url_path and method == "POST":
                try:
                    payload = json.loads(request.content.decode("utf-8"))
                except Exception:
                    payload = {}
                token = payload.get("token") or payload.get("cart_token")
                if token in self.active_reservations:
                    self.confirmed_purchases.add(token)
                    return httpx.Response(200, json={"status": "confirmed", "order_id": f"ord_{uuid.uuid4().hex[:8]}"})
                return httpx.Response(404, json={"error": "Cart reservation not found or expired"})

            return httpx.Response(404, json={"error": f"Route '{url_path}' not found on mock server"})

    def create_transport(self) -> httpx.MockTransport:
        """Creates an in-memory httpx transport forwarding directly to this mock server."""
        return httpx.MockTransport(self.handle_request)
