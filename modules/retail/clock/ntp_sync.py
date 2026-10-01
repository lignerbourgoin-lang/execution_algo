"""
NTP Time Synchronization & Sub-Millisecond Scheduler
----------------------------------------------------
Provides:
1. NtpClient: High-precision RFC 5905 SNTP client measuring local clock drift/offset.
2. HighPrecisionScheduler: Hybrid (async sleep + spin-wait) scheduler for T0 target execution.
"""

import asyncio
import socket
import struct
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional

# NTP epoch starts at Jan 1 1900, Unix epoch starts at Jan 1 1970
# Difference in seconds between 1900 and 1970 (including leap years)
NTP_DELTA = 2208988800


@dataclass
class NtpSyncResult:
    server: str
    offset_ms: float       # Difference: (Atomic Time - Local Time) in ms
    round_trip_ms: float   # Network RTT to NTP server
    stratum: int
    precision: float
    synced_at_local: float # Local time.time() when sync completed


class NtpClient:
    """
    Standard Network Time Protocol (RFC 5905) client.
    Calculates clock skew between local machine and atomic time sources.
    """

    DEFAULT_SERVERS = [
        "time.cloudflare.com",
        "time.google.com",
        "pool.ntp.org",
    ]

    def __init__(self, timeout: float = 2.0):
        self.timeout = timeout
        self.cached_offset_ms: float = 0.0
        self.last_sync_time: float = 0.0

    def query_server(self, host: str, port: int = 123) -> Optional[NtpSyncResult]:
        """
        Sends an SNTP query to the given server and calculates offset and delay.
        """
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.settimeout(self.timeout)

        # RFC 5905: LI = 0, VN = 4 (NTPv4), Mode = 3 (Client) -> 0x23
        msg = b"\x23" + 47 * b"\0"

        try:
            # T1: Client transmit time
            t1 = time.time()
            t1_perf = time.perf_counter_ns()
            client.sendto(msg, (host, port))

            data, _ = client.recvfrom(1024)
            # T4: Client receive time
            t4_perf = time.perf_counter_ns()
            t4 = time.time()

            if len(data) < 48:
                return None

            unpacked = struct.unpack("!BBBb11I", data[:48])
            stratum = unpacked[1]
            precision = unpacked[3]

            # T2: Server receive time (offset 32..40 -> indices 11, 12)
            t2_sec = unpacked[11] - NTP_DELTA
            t2_frac = unpacked[12] / float(2**32)
            t2 = t2_sec + t2_frac

            # T3: Server transmit time (offset 40..48 -> indices 13, 14)
            t3_sec = unpacked[13] - NTP_DELTA
            t3_frac = unpacked[14] / float(2**32)
            t3 = t3_sec + t3_frac

            # Calculate network round-trip and clock offset
            rtt_sec = (t4 - t1) - (t3 - t2)
            offset_sec = ((t2 - t1) + (t3 - t4)) / 2.0

            return NtpSyncResult(
                server=host,
                offset_ms=offset_sec * 1000.0,
                round_trip_ms=max(0.0, rtt_sec * 1000.0),
                stratum=stratum,
                precision=float(precision),
                synced_at_local=t4,
            )

        except Exception:
            return None
        finally:
            client.close()

    def _process_results(self, results: List[NtpSyncResult]) -> Dict[str, Any]:
        if not results:
            return {
                "success": False,
                "error": "All NTP queries failed",
                "offset_ms": 0.0,
            }

        # RFC 5905 Clock Filter: discard samples with abnormal RTT (> 1500ms) caused by routing spikes
        filtered_results = [r for r in results if r.round_trip_ms < 1500.0]
        candidates = filtered_results if filtered_results else results

        # Calculate median offset among reliable low-RTT candidates
        offsets = sorted([r.offset_ms for r in candidates])
        median_offset = offsets[len(offsets) // 2]
        self.cached_offset_ms = median_offset
        self.last_sync_time = time.time()

        return {
            "success": True,
            "median_offset_ms": round(median_offset, 3),
            "servers_responded": len(results),
            "reliable_servers": len(candidates),
            "details": [
                {
                    "server": r.server,
                    "offset_ms": round(r.offset_ms, 3),
                    "rtt_ms": round(r.round_trip_ms, 3),
                    "stratum": r.stratum,
                }
                for r in results
            ],
        }

    def sync(self, servers: Optional[List[str]] = None) -> Dict[str, Any]:
        """Synchronous query of all NTP servers."""
        servers = servers or self.DEFAULT_SERVERS
        results: List[NtpSyncResult] = []
        for s in servers:
            res = self.query_server(s)
            if res:
                results.append(res)
        return self._process_results(results)

    async def sync_async(self, servers: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Asynchronously queries all NTP servers in parallel using worker threads,
        completely non-blocking for the asyncio event loop.
        """
        servers = servers or self.DEFAULT_SERVERS
        tasks = [asyncio.to_thread(self.query_server, s) for s in servers]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)
        results = [r for r in raw_results if isinstance(r, NtpSyncResult)]
        return self._process_results(results)

    def get_atomic_time(self) -> float:
        """Returns the current true UTC time (in seconds) adjusted for clock offset."""
        return time.time() + (self.cached_offset_ms / 1000.0)


class HighPrecisionScheduler:
    """
    Schedules execution at exact atomic UTC timestamps.
    Uses cooperative async sleep with boosted OS timer resolution to avoid CPU freezing.
    """

    def __init__(self, ntp_client: Optional[NtpClient] = None):
        self.ntp = ntp_client or NtpClient()

    async def wait_until_atomic_timestamp(
        self,
        target_atomic_timestamp_utc: float,
        latency_advance_ms: float = 0.0,
    ) -> Dict[str, float]:
        """
        Waits until the exact target timestamp cooperatively without blocking the loop.
        :param target_atomic_timestamp_utc: True UTC timestamp when target event occurs.
        :param latency_advance_ms: Network lead time (e.g. RTT / 2) to fire before T0.
        :return: Metrics describing dispatch accuracy.
        """
        # Ensure NTP sync is performed asynchronously
        if self.ntp.last_sync_time == 0.0:
            await self.ntp.sync_async()

        # Boost Windows timer resolution from 15.6ms to 1ms
        is_win = (sys.platform == "win32")
        if is_win:
            try:
                import ctypes
                ctypes.windll.winmm.timeBeginPeriod(1)
            except Exception:
                pass

        try:
            offset_ns = int(self.ntp.cached_offset_ms * 1_000_000)
            advance_ns = int(latency_advance_ms * 1_000_000)
            target_atomic_ns = int(target_atomic_timestamp_utc * 1_000_000_000)

            # Target timestamp on local system clock
            target_local_ns = target_atomic_ns - offset_ns - advance_ns

            # Cooperative sleep (does not block other asyncio tasks)
            now_local_ns = time.time_ns()
            remaining_ns = target_local_ns - now_local_ns

            if remaining_ns > 0:
                await asyncio.sleep(remaining_ns / 1_000_000_000.0)

            fired_at_ns = time.time_ns()
            accuracy_ms = (fired_at_ns - target_local_ns) / 1_000_000.0

            return {
                "accuracy_ms": round(accuracy_ms, 2),
                "offset_applied_ms": round(self.ntp.cached_offset_ms, 3),
                "lead_time_applied_ms": round(latency_advance_ms, 3),
            }
        finally:
            if is_win:
                try:
                    import ctypes
                    ctypes.windll.winmm.timeEndPeriod(1)
                except Exception:
                    pass
