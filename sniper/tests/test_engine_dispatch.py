from __future__ import annotations

import unittest
from typing import Any

from sniper.engine import Engine
from sniper.models import EventType, HoldResult, Offer, Target


class MockAdapter:
    def __init__(self, offers: list[Offer] | None = None, hold_ok: bool = True) -> None:
        self.offers = offers or [Offer(id="1", category="CAT1", price=100.0, available=5)]
        self.hold_ok = hold_ok
        self.held: list[tuple[str, int]] = []
        self.warmed = False
        self.admitted = False

    async def warmup(self) -> None:
        self.warmed = True

    async def read_cap(self) -> int | None:
        return None

    async def inventory(self, target: Target) -> list[Offer]:
        return self.offers

    async def hold(self, target: Target, offer: Offer, qty: int) -> HoldResult:
        self.held.append((offer.id, qty))
        if self.hold_ok:
            return HoldResult(ok=True, qty=qty, offer_id=offer.id, checkout_url="https://pay.example/1")
        return HoldResult(ok=False, offer_id=offer.id, error="failed")

    async def checkout_url(self, hold: HoldResult) -> str:
        return hold.checkout_url

    async def wait_until_admitted(self, target: Target) -> None:
        self.admitted = True

    async def aclose(self) -> None:
        pass


class RecordingNotify:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def urgent(self, message: str) -> None:
        self.messages.append(message)


class TestEngineDispatch(unittest.IsolatedAsyncioTestCase):
    def _target(self, event_type: EventType, qty: int = 5, cap: int = 6) -> Target:
        return Target(
            id="test_ev",
            type=event_type,
            adapter="generic_json",
            quantity=qty,
            account_cap=cap,
            categories=["CAT1"],
            urls={},
            session_headers={},
            session_cookies={},
            burst_shots=2,
            burst_spacing=0.01,
            t0=0.0,
        )

    async def test_dry_run_does_not_hold(self):
        target = self._target(EventType.DROP)
        notify = RecordingNotify()
        engine = Engine(target, notify)
        mock_adapter = MockAdapter()
        engine.adapter = mock_adapter

        res = await engine.run(dry_run=True)
        self.assertIsNone(res)
        self.assertTrue(mock_adapter.warmed)
        self.assertEqual(len(mock_adapter.held), 0)
        self.assertTrue(any("dry-run" in m for m in notify.messages))

    async def test_after_queue_flow(self):
        target = self._target(EventType.AFTER_QUEUE, qty=3, cap=4)
        notify = RecordingNotify()
        engine = Engine(target, notify)
        mock_adapter = MockAdapter()
        engine.adapter = mock_adapter

        res = await engine.run()
        self.assertIsNotNone(res)
        self.assertTrue(res.ok)
        self.assertTrue(mock_adapter.admitted)
        self.assertEqual(mock_adapter.held, [("1", 3)])


if __name__ == "__main__":
    unittest.main()
