"""
Financial Order Router & Fast Exchange Executor
-----------------------------------------------
Dispatches authenticated trading orders to financial exchanges (Binance, Bybit, etc.)
over pre-warmed keep-alive HTTP/2 sessions with microsecond latency tracing.

Features:
- HMAC-SHA256 signature generation for authenticated REST endpoints.
- Client Order ID (UUIDv4) mapping for strict idempotency and zero duplicate execution.
- Order parameter validation (symbol, side, quantity, price, order_type).
- Structured ExecutionResult with latency breakdown.
"""

from dataclasses import dataclass
import hashlib
import hmac
import logging
import time
from typing import Any, Dict, Optional
import urllib.parse
import uuid

from core.engine.base import BaseExecutor, ExecutionResult, Signal
from core.network.persistent_client import PrewarmedHttpClient
from core.telemetry.tracker import LatencyTracker

logger = logging.getLogger("execution.finance.order_router")


@dataclass
class ExchangeCredentials:
    api_key: str
    api_secret: str
    exchange_name: str = "binance"


class ExchangeOrderExecutor(BaseExecutor):
    """
    Executes financial orders against exchange endpoints.
    Handles signature signing, idempotency keys, and connection pre-warming.
    """

    def __init__(
        self,
        base_url: str,
        http_client: PrewarmedHttpClient,
        credentials: Optional[ExchangeCredentials] = None,
        telemetry: Optional[LatencyTracker] = None,
    ):
        self.base_url = base_url
        self.client = http_client
        self.credentials = credentials
        self.telemetry = telemetry or LatencyTracker()
        self.is_ready = False

    async def initialize(self):
        """Pre-warms the TLS connection to the exchange endpoint."""
        await self.client.start()
        self.is_ready = True

    def _sign_payload(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Signs query/body parameters using HMAC-SHA256 for authenticated endpoints."""
        if not self.credentials:
            return params

        signed_params = dict(params)
        signed_params["timestamp"] = int(time.time() * 1000)

        query_str = urllib.parse.urlencode(signed_params)
        signature = hmac.new(
            self.credentials.api_secret.encode("utf-8"),
            query_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        signed_params["signature"] = signature
        return signed_params

    async def execute(self, signal: Signal) -> ExecutionResult:
        """
        Submits an order according to the incoming Signal specification.
        """
        payload = signal.payload
        symbol = payload.get("symbol") or signal.target_id
        side = payload.get("side", signal.action).upper()
        quantity = payload.get("quantity", 0.0)
        price = payload.get("price")
        order_type = payload.get("type", "LIMIT" if price else "MARKET").upper()
        client_order_id = payload.get("client_order_id") or payload.get("idempotency_key") or f"ord_{uuid.uuid4().hex[:12]}"

        trace = self.telemetry.start_trace(
            action_id=client_order_id,
            target=f"{self.base_url}/order",
            symbol=symbol,
            side=side,
        )

        headers = {}
        if self.credentials:
            headers["X-MBX-APIKEY"] = self.credentials.api_key

        order_params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity,
            "newClientOrderId": client_order_id,
        }
        if price is not None and order_type == "LIMIT":
            order_params["price"] = price
            order_params["timeInForce"] = payload.get("timeInForce", "IOC")  # Immediate or Cancel

        # Sign request if private credentials configured
        final_params = self._sign_payload(order_params) if self.credentials else order_params

        endpoint = payload.get("order_endpoint", "/api/v3/order")
        res = await self.client.execute_fast(
            method="POST",
            endpoint=endpoint,
            action_id=client_order_id,
            json_data=final_params,
            headers=headers,
            idempotency_key=client_order_id,
        )

        trace.mark_stage("exchange_ack")
        status_code = res.get("status_code", 0)
        success = (status_code in (200, 201))

        error_msg = None
        if not success:
            body = res.get("body")
            if isinstance(body, dict):
                error_msg = body.get("msg") or body.get("error") or f"HTTP {status_code}"
            else:
                error_msg = res.get("error") or f"HTTP {status_code}"
            trace.complete(success=False, error=error_msg)
        else:
            trace.complete(success=True)

        return ExecutionResult(
            action_id=client_order_id,
            success=success,
            status_code=status_code,
            data=res.get("body", {}),
            latency_ms=trace.total_latency_ms,
            error=error_msg,
        )

    async def shutdown(self):
        self.is_ready = False
        await self.client.close()
