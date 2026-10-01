"""
Headless Browser Queue Worker & API Handoff Bridge
--------------------------------------------------
Automates the waiting room lifecycle via real Chromium / Chrome instances:
1. Executes client-side JavaScript challenges and proof-of-work legitimately.
2. Holds persistent position in virtual waiting rooms (Queue-It, Cloudflare, etc.).
3. Detects admission status transition in real-time.
4. Performs instantaneous zero-overhead handoff of signed session cookies
   to the low-latency PrewarmedHttpClient engine for sub-millisecond carting.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging
import os
import shutil
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("execution.retail.tickets.queue_worker")

CHROME_DEFAULT_WINDOWS_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
]


def find_chrome_executable() -> Optional[str]:
    """Locates the installed Google Chrome binary on the local system."""
    for path in CHROME_DEFAULT_WINDOWS_PATHS:
        if os.path.isfile(path):
            return path
    which_chrome = shutil.which("chrome") or shutil.which("google-chrome")
    return which_chrome if which_chrome and os.path.isfile(which_chrome) else None


@dataclass
class AdmissionHandoff:
    """Session credentials extracted from browser at the moment of queue admission."""

    worker_id: str
    cookies: Dict[str, str]
    current_url: str
    user_agent: str
    admitted_at_timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueueWorkerConfig:
    """Configuration for an isolated headless browser worker."""

    worker_id: str
    target_queue_url: str
    proxy_url: Optional[str] = None
    user_data_dir: Optional[str] = None
    headless: bool = True
    admission_url_keywords: List[str] = field(default_factory=lambda: ["/checkout", "/shop", "/reserve", "/cart"])
    admission_cookie_keywords: List[str] = field(default_factory=lambda: ["QueueITAccepted", "session", "token"])
    poll_interval_sec: float = 1.0
    timeout_sec: float = 3600.0
    chrome_binary_path: Optional[str] = None


# [FEATURE: HEADLESS_QUEUE_WORKER] Real browser queue listener with microsecond API handoff
# Raison: Virtual waiting rooms execute client-side JS and issue cryptographically signed tokens;
#         this bridge holds the queue in Chrome then hands the token to PrewarmedHttpClient for instant carting.
# Attention: Each worker must maintain its own isolated browser context and sticky proxy.
class HeadlessQueueWorker:
    """
    Manages an automated Chrome browser instance through the waiting room.
    """

    def __init__(self, config: QueueWorkerConfig) -> None:
        self.config = config
        self._chrome_path = config.chrome_binary_path or find_chrome_executable()
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self.is_running = False
        self.is_admitted = False
        self.handoff_data: Optional[AdmissionHandoff] = None

    async def start(self) -> None:
        """Launches the Chrome browser instance with anti-detection flags."""
        try:
            from playwright.async_api import async_playwright
        except ImportError as import_err:
            raise RuntimeError(
                "playwright is required for HeadlessQueueWorker (pip install playwright)"
            ) from import_err

        if not self._chrome_path or not os.path.isfile(self._chrome_path):
            raise FileNotFoundError(
                "Google Chrome executable could not be found. Please specify chrome_binary_path."
            )

        self._playwright = await async_playwright().start()

        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ]

        proxy_dict = None
        if self.config.proxy_url:
            proxy_dict = {"server": self.config.proxy_url}

        if self.config.user_data_dir:
            self._context = await self._playwright.chromium.launch_persistent_context(
                user_data_dir=self.config.user_data_dir,
                executable_path=self._chrome_path,
                headless=self.config.headless,
                args=launch_args,
                proxy=proxy_dict,
                viewport={"width": 1280, "height": 800},
            )
            self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        else:
            self._browser = await self._playwright.chromium.launch(
                executable_path=self._chrome_path,
                headless=self.config.headless,
                args=launch_args,
                proxy=proxy_dict,
            )
            self._context = await self._browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
                ),
            )
            self._page = await self._context.new_page()

        self.is_running = True
        logger.info(
            "Worker [%s] started Chrome instance (headless=%s, proxy=%s)",
            self.config.worker_id,
            self.config.headless,
            bool(self.config.proxy_url),
        )

    async def enter_queue(self) -> None:
        """Navigates to the waiting room target page."""
        if not self._page:
            raise RuntimeError("Browser is not started. Call start() first.")

        logger.info("Worker [%s] entering queue at %s", self.config.worker_id, self.config.target_queue_url)
        await self._page.goto(
            self.config.target_queue_url,
            wait_until="domcontentloaded",
            timeout=30000,
        )

    async def extract_current_cookies(self) -> Dict[str, str]:
        """Reads current cookie state from the browser context."""
        if not self._context:
            return {}
        cookies_list = await self._context.cookies()
        return {item["name"]: item["value"] for item in cookies_list}

    def _check_admission_criteria(self, current_url: str, cookies: Dict[str, str]) -> bool:
        """Evaluates whether the worker has exited the waiting room into admission state."""
        for keyword in self.config.admission_url_keywords:
            if keyword.lower() in current_url.lower():
                return True

        for cookie_kw in self.config.admission_cookie_keywords:
            for cookie_name in cookies.keys():
                if cookie_kw.lower() in cookie_name.lower():
                    return True

        return False

    async def wait_until_admitted(self) -> AdmissionHandoff:
        """
        Polls browser state until admission is detected, then extracts tokens immediately.
        """
        if not self._page or not self._context:
            raise RuntimeError("Browser not started.")

        start_time = time.time()
        deadline = start_time + self.config.timeout_sec

        logger.info("Worker [%s] monitoring queue admission (timeout: %.0fs)...", self.config.worker_id, self.config.timeout_sec)

        while time.time() < deadline:
            try:
                current_url = self._page.url
                cookies = await self.extract_current_cookies()

                if self._check_admission_criteria(current_url, cookies):
                    self.is_admitted = True
                    user_agent = await self._page.evaluate("() => navigator.userAgent")
                    self.handoff_data = AdmissionHandoff(
                        worker_id=self.config.worker_id,
                        cookies=cookies,
                        current_url=current_url,
                        user_agent=user_agent,
                        admitted_at_timestamp=time.time(),
                    )
                    logger.info(
                        "Worker [%s] ADMITTED! Token extracted in %.2fs. URL: %s",
                        self.config.worker_id,
                        time.time() - start_time,
                        current_url,
                    )
                    return self.handoff_data
            except Exception as poll_error:
                logger.debug("Worker [%s] poll iteration exception: %s", self.config.worker_id, poll_error)

            await asyncio.sleep(self.config.poll_interval_sec)

        raise TimeoutError(f"Worker [{self.config.worker_id}] admission timed out after {self.config.timeout_sec}s")

    # [FEATURE: IN_BROWSER_FETCH_EXECUTION] Native in-browser reservation firing preserving 100% TLS/JA4 parity
    # Raison: Dispatches the final cart reservation directly within the admitted Chrome instance via window.fetch
    # Attention: Bypasses OpenSSL/Python fingerprint mismatch by reusing Chrome's genuine active TLS/HTTP2 session
    async def execute_in_browser_fetch(
        self,
        endpoint_url: str,
        method: str = "POST",
        payload: Optional[Dict[str, Any]] = None,
        custom_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Executes an HTTP request directly within the admitted Chrome browser context
        using window.fetch().

        Guarantees 100% native TLS (JA3/JA4) and HTTP/2 settings frame parity
        with the active Chrome session without any fingerprint divergence.
        """
        if not self._page:
            raise RuntimeError("Browser page is not initialized. Call start() first.")

        js_code = """
        async ({ url, method, body, headers }) => {
            const fetchOptions = {
                method: method,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json, text/plain, */*',
                    ...headers,
                },
                credentials: 'include',
            };
            if (body && method !== 'GET' && method !== 'HEAD') {
                fetchOptions.body = JSON.stringify(body);
            }
            const startTime = performance.now();
            try {
                const response = await fetch(url, fetchOptions);
                const durationMs = performance.now() - startTime;
                let parsedData = null;
                const contentType = response.headers.get('content-type') || '';
                if (contentType.includes('application/json')) {
                    parsedData = await response.json();
                } else {
                    parsedData = { text: await response.text() };
                }
                return {
                    ok: response.ok,
                    status_code: response.status,
                    data: parsedData,
                    duration_ms: durationMs,
                    headers: Object.fromEntries(response.headers.entries()),
                };
            } catch (err) {
                return {
                    ok: false,
                    status_code: 0,
                    error: err.toString(),
                    duration_ms: performance.now() - startTime,
                };
            }
        }
        """

        args = {
            "url": endpoint_url,
            "method": method.upper(),
            "body": payload,
            "headers": custom_headers or {},
        }

        result = await self._page.evaluate(js_code, args)
        return result

    async def close(self) -> None:
        """Closes browser context and Playwright process cleanly."""
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as close_error:
            logger.debug("Worker [%s] close error: %s", self.config.worker_id, close_error)
        finally:
            self.is_running = False
            logger.info("Worker [%s] closed.", self.config.worker_id)
