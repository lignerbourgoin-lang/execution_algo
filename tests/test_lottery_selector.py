"""
Unit tests for LotteryQueueSelector and MultiIpLotteryOrchestrator
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.network.ip_pool import SubnetIpPool
from modules.retail.tickets.lottery_selector import (
    LotteryQueueSelector,
    LotteryTicket,
    MultiIpLotteryOrchestrator,
)


class TestLotteryQueueSelector(unittest.TestCase):
    def setUp(self):
        self.selector = LotteryQueueSelector(lower_is_better=True)

    def test_register_and_sort_queue_numbers(self):
        # Register several IPs with queue positions
        self.selector.register_ticket("192.168.1.1", 4500)
        self.selector.register_ticket("192.168.1.2", 12)
        self.selector.register_ticket("192.168.1.3", 350)
        self.selector.register_ticket("192.168.1.4", 89000)

        self.assertEqual(len(self.selector), 4)

        # Single best ticket
        best = self.selector.best_ticket()
        self.assertIsNotNone(best)
        self.assertEqual(best.ip_address, "192.168.1.2")
        self.assertEqual(best.queue_number, 12)

        # Top 2 best tickets
        top_2 = self.selector.get_best_tickets(top_k=2)
        self.assertEqual(len(top_2), 2)
        self.assertEqual(top_2[0].queue_number, 12)
        self.assertEqual(top_2[1].queue_number, 350)

    def test_max_acceptable_position_cutoff(self):
        selector = LotteryQueueSelector(lower_is_better=True, max_acceptable_position=1000)
        selector.register_ticket("10.0.0.1", 50)
        selector.register_ticket("10.0.0.2", 999)
        selector.register_ticket("10.0.0.3", 1001)
        selector.register_ticket("10.0.0.4", 50000)

        best = selector.get_best_tickets()
        self.assertEqual(len(best), 2)
        self.assertEqual([t.queue_number for t in best], [50, 999])

    def test_prune_non_viable(self):
        self.selector.register_ticket("1.1.1.1", 100)
        self.selector.register_ticket("1.1.1.2", 500)
        self.selector.register_ticket("1.1.1.3", 2000)
        self.selector.register_ticket("1.1.1.4", 15000)

        kept, discarded = self.selector.prune_non_viable(keep_top_k=2, max_position=5000)

        self.assertEqual(len(kept), 2)
        self.assertEqual([t.queue_number for t in kept], [100, 500])
        self.assertTrue(all(t.status == "selected" for t in kept))

        self.assertEqual(len(discarded), 2)
        self.assertEqual([t.queue_number for t in discarded], [2000, 15000])
        self.assertTrue(all(t.status == "discarded" for t in discarded))

    def test_score_mode_higher_is_better(self):
        selector_score = LotteryQueueSelector(lower_is_better=False)
        selector_score.register_ticket("10.0.0.1", 20)
        selector_score.register_ticket("10.0.0.2", 950)
        selector_score.register_ticket("10.0.0.3", 400)

        best = selector_score.best_ticket()
        self.assertIsNotNone(best)
        self.assertEqual(best.ip_address, "10.0.0.2")
        self.assertEqual(best.queue_number, 950)

    def test_invalid_inputs_fail_closed(self):
        with self.assertRaises(TypeError):
            self.selector.register_ticket("10.0.0.1", "first")  # type: ignore

        with self.assertRaises(ValueError):
            self.selector.register_ticket("10.0.0.1", 0)  # queue rank must be >= 1


class TestMultiIpLotteryOrchestrator(unittest.IsolatedAsyncioTestCase):
    async def test_survey_pool_selects_best_ips(self):
        pool = SubnetIpPool("192.168.10.0/29")  # 6 usable hosts (.1 to .6)

        # Mock queue response for each IP
        simulated_positions = {
            "192.168.10.1": 45000,
            "192.168.10.2": 85,      # Winner 2
            "192.168.10.3": 12000,
            "192.168.10.4": 14,      # Winner 1
            "192.168.10.5": 99999,
            "192.168.10.6": 350,     # Winner 3
        }

        async def mock_query(ip: str) -> int:
            await asyncio.sleep(0.01)
            return simulated_positions.get(ip, 999999)

        orchestrator = MultiIpLotteryOrchestrator(concurrency_limit=3)
        winners = await orchestrator.survey_pool(pool, mock_query, top_k=3)

        self.assertEqual(len(winners), 3)
        self.assertEqual(winners[0].ip_address, "192.168.10.4")
        self.assertEqual(winners[0].queue_number, 14)
        self.assertEqual(winners[1].ip_address, "192.168.10.2")
        self.assertEqual(winners[1].queue_number, 85)
        self.assertEqual(winners[2].ip_address, "192.168.10.6")
        self.assertEqual(winners[2].queue_number, 350)


if __name__ == "__main__":
    unittest.main()
