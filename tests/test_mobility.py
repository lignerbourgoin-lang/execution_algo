"""
Unit Tests for Mobility Slot Sniping Module
-------------------------------------------
"""

from datetime import datetime
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.engine.base import Signal
from modules.mobility.slot_sniper import (
    SlotBookingExecutor,
    SlotRequirement,
    SlotSniperStrategy,
)


class TestSlotSniper(unittest.TestCase):
    def setUp(self):
        self.req = SlotRequirement(
            earliest_date=datetime(2026, 10, 5, 8, 0),
            latest_date=datetime(2026, 10, 15, 18, 0),
            preferred_center_ids=["center_paris_01", "center_paris_02"],
            user_id="user_eliott_42",
            booking_token="tok_booking_secure",
        )
        self.strategy = SlotSniperStrategy(self.req)

    def test_valid_slot_generates_claim_signal(self):
        opening = {
            "slot_id": "slot_987",
            "center_id": "center_paris_01",
            "datetime_iso": "2026-10-10T10:30:00",
        }
        signal = self.strategy.evaluate(opening)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "CLAIM_SLOT")
        self.assertEqual(signal.payload["slot_id"], "slot_987")
        self.assertEqual(signal.payload["user_id"], "user_eliott_42")

    def test_disallowed_center_rejected(self):
        opening = {
            "slot_id": "slot_988",
            "center_id": "center_lyon_05",  # Not in preferred centers
            "datetime_iso": "2026-10-10T10:30:00",
        }
        self.assertIsNone(self.strategy.evaluate(opening))

    def test_out_of_range_date_rejected(self):
        # Too late
        opening = {
            "slot_id": "slot_989",
            "center_id": "center_paris_01",
            "datetime_iso": "2026-11-01T10:30:00",
        }
        self.assertIsNone(self.strategy.evaluate(opening))


if __name__ == "__main__":
    unittest.main()
