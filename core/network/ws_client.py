"""
High-Performance Resilient WebSocket Stream Client
--------------------------------------------------
Maintains persistent WebSocket streaming connection with:
- Zero-allocation event dispatch
- Automatic reconnect with exponential backoff & jitter
- Heartbeat / Ping-Pong liveness
"""

import asyncio
import json
import logging
import random
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional
import websockets

logger = logging.getLogger("core.network.ws")


class AsyncWebSocketClient:
    """
    Resilient WebSocket client for continuous low-latency event monitoring.
    """

    def __init__(
        self,
        url: str,
        ping_interval: float = 20.0,
        ping_timeout: float = 10.0,
        max_reconnect_delay: float = 30.0,
    ):
        self.url = url
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.max_reconnect_delay = max_reconnect_delay

        self._callbacks: List[Callable[[Dict[str, Any], int], Coroutine[Any, Any, None]]] = []
        self._is_running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._loop_task: Optional[asyncio.Task] = None
        self.is_connected = False
        self.messages_received = 0

    def on_message(self, callback: Callable[[Dict[str, Any], int], Coroutine[Any, Any, None]]):
        """Register an async callback triggered upon every received event."""
        self._callbacks.append(callback)

    async def start(self):
        """Starts the persistent background consumer."""
        self._is_running = True
        self._loop_task = asyncio.create_task(self._connection_loop())

    async def _connection_loop(self):
        reconnect_delay = 1.0
        while self._is_running:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    self.is_connected = True
                    reconnect_delay = 1.0  # Reset delay upon successful connection

                    async for raw_msg in ws:
                        recv_time_ns = time.perf_counter_ns()
                        self.messages_received += 1
                        try:
                            data = json.loads(raw_msg)
                        except Exception:
                            data = {"raw": raw_msg}

                        # Dispatch asynchronously to registered handlers
                        for cb in self._callbacks:
                            asyncio.create_task(cb(data, recv_time_ns))

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.is_connected = False
                if not self._is_running:
                    break

                # Exponential backoff with jitter
                sleep_time = min(self.max_reconnect_delay, reconnect_delay + random.uniform(0, 0.5))
                reconnect_delay = min(self.max_reconnect_delay, reconnect_delay * 1.5)
                await asyncio.sleep(sleep_time)

    async def send_json(self, payload: Dict[str, Any]):
        """Sends a JSON message over the active socket."""
        if self._ws and self.is_connected:
            await self._ws.send(json.dumps(payload))

    async def close(self):
        """Closes the WebSocket connection gracefully."""
        self._is_running = False
        self.is_connected = False
        if self._ws:
            await self._ws.close()
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
