import unittest

from sniper.models import EventType, Target


class QtyTest(unittest.TestCase):
    def _t(self, quantity: int, cap: int) -> Target:
        return Target(
            id="t",
            type=EventType.DROP,
            adapter="generic_json",
            quantity=quantity,
            account_cap=cap,
            categories=[],
            urls={},
            session_headers={},
            session_cookies={},
        )

    def test_min(self):
        self.assertEqual(self._t(5, 6).buy_qty, 5)
        self.assertEqual(self._t(5, 4).buy_qty, 4)
        self.assertEqual(self._t(5, 0).buy_qty, 0)


if __name__ == "__main__":
    unittest.main()
