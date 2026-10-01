from __future__ import annotations

import time
import unittest
from unittest.mock import patch

from sniper.clock import Clock


class TestClock(unittest.TestCase):
    def test_clock_now_with_offset(self):
        clock = Clock()
        clock.offset = 1.5
        t_before = time.time()
        c_now = clock.now()
        t_after = time.time()
        self.assertGreaterEqual(c_now, t_before + 1.49)
        self.assertLessEqual(c_now, t_after + 1.51)

    def test_sleep_until_past(self):
        clock = Clock()
        clock.offset = 0.0
        # Target in the past returns immediately
        start = time.perf_counter()
        clock.sleep_until(time.time() - 1.0)
        elapsed = time.perf_counter() - start
        self.assertLess(elapsed, 0.1)

    def test_sleep_until_short_future(self):
        clock = Clock()
        clock.offset = 0.0
        target = time.time() + 0.02
        clock.sleep_until(target)
        self.assertGreaterEqual(time.time(), target)


if __name__ == "__main__":
    unittest.main()
