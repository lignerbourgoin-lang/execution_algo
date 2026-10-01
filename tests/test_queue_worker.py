"""
Unit tests for HeadlessQueueWorker and admission criteria detection
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from modules.retail.tickets.queue_worker import (
    AdmissionHandoff,
    HeadlessQueueWorker,
    QueueWorkerConfig,
    find_chrome_executable,
)


class TestHeadlessQueueWorker(unittest.TestCase):
    def test_find_chrome_executable(self):
        chrome_path = find_chrome_executable()
        # On Windows standard machine, Chrome should be found
        if chrome_path:
            self.assertTrue(os.path.isfile(chrome_path))
            self.assertTrue(chrome_path.lower().endswith("chrome.exe"))

    def test_admission_criteria_url_match(self):
        config = QueueWorkerConfig(
            worker_id="test_worker_01",
            target_queue_url="https://queue.example.com/waiting",
            admission_url_keywords=["/checkout", "/shop", "/reserve"],
        )
        worker = HeadlessQueueWorker(config)

        # In waiting room: should NOT be admitted
        self.assertFalse(worker._check_admission_criteria(
            current_url="https://queue.example.com/waiting?q=12345",
            cookies={"test_queue_id": "abc"},
        ))

        # Redirected to checkout: should be admitted
        self.assertTrue(worker._check_admission_criteria(
            current_url="https://tickets.example.com/event/101/checkout",
            cookies={},
        ))

    def test_admission_criteria_cookie_match(self):
        config = QueueWorkerConfig(
            worker_id="test_worker_02",
            target_queue_url="https://queue.example.com/waiting",
            admission_cookie_keywords=["QueueITAccepted", "admit_token"],
        )
        worker = HeadlessQueueWorker(config)

        # Without admission cookie
        self.assertFalse(worker._check_admission_criteria(
            current_url="https://queue.example.com/waiting",
            cookies={"_ga": "GA1.2.3456", "session_id": "xyz"},
        ))

        # When QueueITAccepted token is issued by the server
        self.assertTrue(worker._check_admission_criteria(
            current_url="https://queue.example.com/waiting",
            cookies={"_ga": "GA1.2.3456", "QueueITAccepted-SDOuter-1234": "signed_token_payload"},
        ))

    def test_missing_chrome_fail_closed(self):
        config = QueueWorkerConfig(
            worker_id="test_worker_fail",
            target_queue_url="https://example.com",
            chrome_binary_path=r"C:\NonExistentDirectory\FakeChrome.exe",
        )
        worker = HeadlessQueueWorker(config)

        import asyncio

        async def run_failing_start():
            await worker.start()

        with self.assertRaises(FileNotFoundError):
            asyncio.run(run_failing_start())


if __name__ == "__main__":
    unittest.main()
