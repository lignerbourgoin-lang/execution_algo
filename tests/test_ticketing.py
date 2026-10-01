"""
Unit Tests for the ticketing tools and the hardened core
--------------------------------------------------------
Conditional poller, resale watcher, opening reminder, notifiers, config validation,
rate limiter Retry-After parsing, HTTP client HTTP/2 guard and orchestrator task tracking.
All offline: HTTP is served by httpx.MockTransport.
"""

import asyncio
import email.utils
import json
import os
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

import httpx

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.config import ConfigError, load_task_config, parse_aware_datetime
from core.engine.base import BaseExecutor, BaseStrategy, Signal
from core.engine.orchestrator import ExecutionOrchestrator
from core.network.persistent_client import PrewarmedHttpClient
from core.rate_limiter.limiter import AdaptiveRateLimiter, parse_retry_after_seconds
from modules.retail.monitors.conditional_poll import ConditionalPoller
from modules.retail.notify.notifiers import Notification, NtfyNotifier, broadcast
from modules.retail.opening.reminder import OpeningReminder, OpeningReminderConfig
from modules.retail.resale.watcher import (
    ListingFieldPaths,
    ListingFilters,
    ResaleWatchConfig,
    ResaleWatcher,
    extract_path,
    matches_filters,
    parse_listings,
)


class RecordingNotifier:
    def __init__(self, name="recorder", fail=False):
        self.name = name
        self.fail = fail
        self.sent = []

    async def send(self, notification):
        if self.fail:
            raise RuntimeError("channel down")
        self.sent.append(notification)


def json_response(document, status_code=200, headers=None):
    return httpx.Response(status_code, content=json.dumps(document).encode(), headers={"content-type": "application/json", **(headers or {})})


class TestConditionalPoller(unittest.IsolatedAsyncioTestCase):
    async def test_detects_change_without_etag_or_last_modified(self):
        documents = [{"stock": 0}, {"stock": 0}, {"stock": 3}]

        def handler(request):
            return json_response(documents.pop(0))

        poller = ConditionalPoller("https://example.com/stock", poll_interval_sec=1.0, http2=False)
        received = []

        async def on_change(data, received_ns):
            received.append(data)

        poller.on_change(on_change)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            for _ in range(3):
                await poller.poll_once(client)
        await poller._callback_tasks.wait_all()
        self.assertEqual(received, [{"stock": 3}])

    async def test_sends_if_none_match_and_counts_304(self):
        seen_headers = []

        def handler(request):
            seen_headers.append(request.headers.get("if-none-match"))
            if request.headers.get("if-none-match") == '"v1"':
                return httpx.Response(304)
            return json_response({"v": 1}, headers={"etag": '"v1"'})

        poller = ConditionalPoller("https://example.com/r", poll_interval_sec=1.0, http2=False)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await poller.poll_once(client)
            await poller.poll_once(client)
        self.assertEqual(seen_headers, [None, '"v1"'])
        self.assertEqual(poller.not_modified_count, 1)

    async def test_backs_off_on_503(self):
        def handler(request):
            return httpx.Response(503, headers={"retry-after": "7"})

        poller = ConditionalPoller("https://example.com/r", poll_interval_sec=1.0, http2=False)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await poller.poll_once(client)
        remaining_penalty_sec = (poller.rate_limiter.penalty_until_ns - time.perf_counter_ns()) / 1e9
        self.assertGreater(remaining_penalty_sec, 6.0)

    def test_rejects_aggressive_interval(self):
        with self.assertRaises(ValueError):
            ConditionalPoller("https://example.com/r", poll_interval_sec=0.1)


FEED_FIELDS = ListingFieldPaths(listing_id="id", price="price.amount", title="category", url="url", quantity="qty")


def make_feed(*listings):
    return {"data": {"listings": list(listings)}}


def make_listing(listing_id, price, category="Fosse", qty=1):
    return {"id": listing_id, "price": {"amount": price}, "category": category, "url": f"https://r.example/{listing_id}", "qty": qty}


class TestResaleParsing(unittest.TestCase):
    def test_extract_path(self):
        self.assertEqual(extract_path({"a": [{"b": 5}]}, "a.0.b"), 5)
        self.assertIsNone(extract_path({"a": {}}, "a.b.c"))
        self.assertEqual(extract_path([1, 2], ""), [1, 2])

    def test_price_is_decimal_without_float_noise(self):
        listings = parse_listings(make_feed(make_listing(1, 49.9)), "data.listings", FEED_FIELDS)
        self.assertEqual(listings[0].price, Decimal("49.9"))

    def test_filters(self):
        filters = ListingFilters(max_price=Decimal("100"), title_keywords=("fosse",), excluded_keywords=("pmr",))
        cheap, expensive, wrong_category, excluded, unknown_price = parse_listings(
            make_feed(
                make_listing(1, 80),
                make_listing(2, 150),
                make_listing(3, 50, category="Gradin"),
                make_listing(4, 50, category="Fosse PMR"),
                {"id": 5, "category": "Fosse"},
            ),
            "data.listings",
            FEED_FIELDS,
        )
        self.assertTrue(matches_filters(cheap, filters))
        self.assertFalse(matches_filters(expensive, filters))
        self.assertFalse(matches_filters(wrong_category, filters))
        self.assertFalse(matches_filters(excluded, filters))
        self.assertFalse(matches_filters(unknown_price, filters), "unknown price must fail closed")

    def test_wrong_items_path_returns_empty(self):
        self.assertEqual(parse_listings({"data": {}}, "data.listings", FEED_FIELDS), [])


class TestResaleWatcher(unittest.IsolatedAsyncioTestCase):
    def make_watcher(self, alert_existing_on_start=True):
        recorder = RecordingNotifier()
        config = ResaleWatchConfig(
            url="https://r.example/feed",
            items_path="data.listings",
            fields=FEED_FIELDS,
            filters=ListingFilters(max_price=Decimal("100")),
            alert_existing_on_start=alert_existing_on_start,
        )
        return ResaleWatcher(config, [recorder]), recorder

    async def test_alerts_once_per_new_matching_listing(self):
        watcher, recorder = self.make_watcher()
        await watcher.handle_document(make_feed(make_listing(1, 90), make_listing(2, 300)), time.perf_counter_ns())
        await watcher.handle_document(make_feed(make_listing(1, 90), make_listing(3, 60)), time.perf_counter_ns())
        self.assertEqual([n.url for n in recorder.sent], ["https://r.example/1", "https://r.example/3"])
        self.assertIsNotNone(watcher.last_alert_latency_ms)

    async def test_existing_listings_can_be_ignored_at_start(self):
        watcher, recorder = self.make_watcher(alert_existing_on_start=False)
        await watcher.handle_document(make_feed(make_listing(1, 90)), time.perf_counter_ns())
        await watcher.handle_document(make_feed(make_listing(1, 90), make_listing(2, 50)), time.perf_counter_ns())
        self.assertEqual([n.url for n in recorder.sent], ["https://r.example/2"])

    async def test_auto_reserve_action_triggers_and_updates_alert(self):
        recorder = RecordingNotifier()
        config = ResaleWatchConfig(
            url="https://r.example/feed",
            items_path="data.listings",
            fields=FEED_FIELDS,
            filters=ListingFilters(max_price=Decimal("100")),
        )
        reserved_items = []

        async def mock_auto_reserve(listing):
            reserved_items.append(listing.listing_id)
            return {"checkout_url": f"https://r.example/checkout/{listing.listing_id}"}

        watcher = ResaleWatcher(config, [recorder], auto_reserve_action=mock_auto_reserve)
        await watcher.handle_document(make_feed(make_listing(42, 85)), time.perf_counter_ns())

        self.assertEqual(reserved_items, ["42"])
        self.assertEqual(len(recorder.sent), 1)
        self.assertIn("PANIER VERROUILLÉ", recorder.sent[0].title)
        self.assertEqual(recorder.sent[0].url, "https://r.example/checkout/42")

    def test_requires_a_notifier(self):
        with self.assertRaises(ValueError):
            ResaleWatcher(ResaleWatchConfig(url="https://r.example/feed"), [])


class TestNotifiers(unittest.IsolatedAsyncioTestCase):
    async def test_one_failing_channel_does_not_block_others(self):
        healthy, broken = RecordingNotifier("healthy"), RecordingNotifier("broken", fail=True)
        failed_channels = await broadcast([broken, healthy], Notification("t", "m"))
        self.assertEqual(failed_channels, ["broken"])
        self.assertEqual(len(healthy.sent), 1)

    async def test_ntfy_publishes_json_with_click_url(self):
        captured = {}

        def handler(request):
            captured.update(json.loads(request.content))
            return httpx.Response(200)

        notifier = NtfyNotifier("a-very-long-random-topic", transport=httpx.MockTransport(handler))
        await notifier.send(Notification("Revente : Fosse", "80 EUR", url="https://r.example/1", is_urgent=True))
        await notifier.close()
        self.assertEqual(captured["topic"], "a-very-long-random-topic")
        self.assertEqual(captured["click"], "https://r.example/1")
        self.assertEqual(captured["priority"], 5)

    def test_short_ntfy_topic_rejected(self):
        with self.assertRaises(ValueError):
            NtfyNotifier("short")


class InstantScheduler:
    def __init__(self):
        self.targets = []

    async def wait_until_atomic_timestamp(self, target_timestamp, latency_advance_ms=0.0):
        self.targets.append(target_timestamp)
        return {"clock_uncertainty_ms": 1.0}


class TestOpeningReminder(unittest.IsolatedAsyncioTestCase):
    async def test_sends_future_reminders_in_order_and_opens_browser_once(self):
        opening_time = datetime.now(timezone.utc) + timedelta(minutes=10)
        config = OpeningReminderConfig(
            event_name="Concert",
            sale_url="https://tickets.example/e",
            opening_time_utc=opening_time,
            reminder_minutes_before=(15, 3, 0),
            open_browser_minutes_before=3,
        )
        recorder, scheduler = RecordingNotifier(), InstantScheduler()
        reminder = OpeningReminder(config, [recorder], scheduler=scheduler)
        reminder.browser = RecordingNotifier("browser")

        await reminder.run()
        self.assertEqual(reminder.sent_reminders, [3, 0], "T-15 is already past and must be skipped")
        self.assertEqual(len(recorder.sent), 2)
        self.assertEqual(len(reminder.browser.sent), 1)
        self.assertEqual(scheduler.targets, sorted(scheduler.targets))

    def test_naive_opening_time_rejected(self):
        with self.assertRaises(ValueError):
            OpeningReminderConfig(event_name="x", sale_url="u", opening_time_utc=datetime(2026, 10, 15, 10, 0))

    def test_browser_offset_must_be_a_reminder(self):
        with self.assertRaises(ValueError):
            OpeningReminderConfig(
                event_name="x",
                sale_url="u",
                opening_time_utc=datetime.now(timezone.utc),
                reminder_minutes_before=(5, 0),
                open_browser_minutes_before=3,
            )


class TestConfig(unittest.TestCase):
    def test_parse_aware_datetime(self):
        parsed = parse_aware_datetime("2026-10-15T10:00:00+02:00", "t")
        self.assertEqual(parsed.astimezone(timezone.utc).hour, 8)
        self.assertEqual(parse_aware_datetime("2026-10-15T08:00:00Z", "t"), parsed)
        with self.assertRaises(ConfigError):
            parse_aware_datetime("2026-10-15T10:00:00", "t")
        with self.assertRaises(ConfigError):
            parse_aware_datetime(1790000000.0, "t")

    def test_scheduled_task_requires_aware_time(self):
        config_path = os.path.join(os.path.dirname(__file__), "_tmp_task.json")
        try:
            with open(config_path, "w", encoding="utf-8") as config_file:
                json.dump({"target": {"base_url": "https://x"}, "scheduling": {"mode": "scheduled", "target_time_utc": "2026-10-15T10:00:00"}}, config_file)
            with self.assertRaises(ConfigError):
                load_task_config(config_path)
        finally:
            os.remove(config_path)

    def test_example_configs_are_valid_json(self):
        config_directory = os.path.join(os.path.dirname(__file__), "..", "config")
        for file_name in os.listdir(config_directory):
            if file_name.endswith(".json.example"):
                with open(os.path.join(config_directory, file_name), encoding="utf-8") as config_file:
                    json.load(config_file)


class TestRetryAfter(unittest.TestCase):
    def test_seconds_and_http_date(self):
        self.assertEqual(parse_retry_after_seconds("12"), 12.0)
        in_thirty_seconds = email.utils.formatdate(time.time() + 30, usegmt=True)
        self.assertAlmostEqual(parse_retry_after_seconds(in_thirty_seconds), 30.0, delta=2.0)
        self.assertIsNone(parse_retry_after_seconds("garbage"))
        self.assertEqual(parse_retry_after_seconds("99999"), 300.0)

    def test_503_penalizes(self):
        limiter = AdaptiveRateLimiter(base_rate=10.0, burst_capacity=10.0)
        limiter.on_response(503, {"retry-after": "1"})
        self.assertLess(limiter.current_rate, 10.0)


class TestPrewarmedHttpClient(unittest.IsolatedAsyncioTestCase):
    def test_http2_without_h2_fails_closed(self):
        with mock.patch("importlib.util.find_spec", return_value=None):
            with self.assertRaises(RuntimeError):
                PrewarmedHttpClient("https://example.com", http2=True)

    async def test_network_error_returns_status_zero(self):
        def handler(request):
            raise httpx.ConnectError("boom", request=request)

        client = PrewarmedHttpClient("https://example.com", http2=False, transport=httpx.MockTransport(handler))
        result = await client.execute_fast("POST", "/cart", action_id="t", json_data={"a": 1}, idempotency_key="k1")
        await client.close()
        self.assertEqual(result["status_code"], 0)
        self.assertIn("ConnectError", result["error"])

    async def test_idempotency_header_sent(self):
        seen_keys = []

        def handler(request):
            seen_keys.append(request.headers.get("idempotency-key"))
            return json_response({"ok": True})

        client = PrewarmedHttpClient("https://example.com", http2=False, transport=httpx.MockTransport(handler))
        result = await client.execute_fast("POST", "/cart", action_id="t", json_data={}, idempotency_key="key-1")
        await client.close()
        self.assertEqual(seen_keys, ["key-1"])
        self.assertEqual(result["body"], {"ok": True})

    async def test_prewarm_custom_probe_path(self):
        seen_paths = []

        def handler(request):
            seen_paths.append(request.url.path)
            return httpx.Response(200, headers={"Content-Type": "application/json"})

        client = PrewarmedHttpClient(
            "https://example.com",
            http2=False,
            transport=httpx.MockTransport(handler),
            probe_path="/api/health",
        )
        prewarm_success = await client.prewarm()
        await client.close()
        self.assertTrue(prewarm_success)
        self.assertIn("/api/health", seen_paths)



class AlwaysSignal(BaseStrategy):
    def evaluate(self, market_data):
        return Signal(source="feed", target_id="boom", action="BUY", payload={})


class ExplodingExecutor(BaseExecutor):
    async def initialize(self):
        pass

    async def execute(self, signal):
        raise RuntimeError("executor crashed")

    async def shutdown(self):
        pass


class TestOrchestratorTaskTracking(unittest.IsolatedAsyncioTestCase):
    async def test_executor_exception_is_logged_not_lost(self):
        orchestrator = ExecutionOrchestrator()
        orchestrator.register_strategy(AlwaysSignal())
        orchestrator.register_executor("boom", ExplodingExecutor())
        await orchestrator.initialize()

        with self.assertLogs("core.tasks", level="ERROR"):
            await orchestrator.on_event({}, event_received_ns=1)
            await orchestrator._dispatch_tasks.wait_all()
            await asyncio.sleep(0)
        self.assertEqual(orchestrator._dispatch_tasks.failed_count, 1)
        await orchestrator.shutdown()


if __name__ == "__main__":
    unittest.main()
