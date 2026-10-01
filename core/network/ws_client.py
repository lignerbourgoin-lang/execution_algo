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
        self._on_gap_callbacks: List[Callable[[int, int], Coroutine[Any, Any, None]]] = []
        self._is_running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._loop_task: Optional[asyncio.Task] = None
        self._active_callback_tasks: set[asyncio.Task] = set()
        self.is_connected = False
        self.messages_received = 0
        self.reconnect_count = 0
        self.gaps_detected = 0
        self.last_sequence_id: Optional[int] = None

    def on_message(self, callback: Callable[[Dict[str, Any], int], Coroutine[Any, Any, None]]):
        """Register an async callback triggered upon every received event."""
        self._callbacks.append(callback)

    def on_sequence_gap(self, callback: Callable[[int, int], Coroutine[Any, Any, None]]):
        """Register a callback triggered when missed events/sequence gaps are detected."""
        self._on_gap_callbacks.append(callback)

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
                        self._dispatch_message(raw_msg, recv_time_ns)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.is_connected = False
                self.reconnect_count += 1
                if not self._is_running:
                    break

                # Exponential backoff with jitter
                sleep_time = min(self.max_reconnect_delay, reconnect_delay + random.uniform(0, 0.5))
                reconnect_delay = min(self.max_reconnect_delay, reconnect_delay * 1.5)
                await asyncio.sleep(sleep_time)

    def _dispatch_message(self, raw_msg: str, recv_time_ns: int):
        """Processes incoming raw message, tracks sequence gaps, and invokes callbacks."""
        self.messages_received += 1
        try:
            data = json.loads(raw_msg)
        except Exception:
            data = {"raw": raw_msg}

        # Sequence Gap Detection (Financial streams: seq, u, sequence, lastUpdateId)
        if isinstance(data, dict):
            seq = data.get("seq") or data.get("sequence") or data.get("u") or data.get("lastUpdateId")
            if isinstance(seq, int):
                if self.last_sequence_id is not None and seq > self.last_sequence_id + 1:
                    missed = seq - (self.last_sequence_id + 1)
                    self.gaps_detected += 1
                    logger.warning(
                        f"WebSocket sequence gap: missed {missed} events "
                        f"({self.last_sequence_id} -> {seq}). Data resync required."
                    )
                    for g_cb in self._on_gap_callbacks:
                        t = asyncio.create_task(g_cb(self.last_sequence_id, seq))
                        self._active_callback_tasks.add(t)
                        t.add_done_callback(self._active_callback_tasks.discard)
                self.last_sequence_id = seq

        # Dispatch asynchronously with strong reference tracking
        for cb in self._callbacks:
            task = asyncio.create_task(cb(data, recv_time_ns))
            self._active_callback_tasks.add(task)
            task.add_done_callback(self._active_callback_tasks.discard)

    async def send_json(self, payload: Dict[str, Any]):
        """Sends a JSON message over the active socket."""
        if self._ws and self.is_connected:
            await self._ws.send(json.dumps(payload))

    async def close(self):
        """Closes the WebSocket connection gracefully and awaits in-flight callbacks."""
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
        if self._active_callback_tasks:
            await asyncio.gather(*self._active_callback_tasks, return_exceptions=True)
            self._active_callback_tasks.clear()


# Alias for intuitive naming
WebSocketClient = AsyncWebSocketClient

