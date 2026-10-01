"""
NTP Time Synchronization & Sub-Millisecond Scheduler
----------------------------------------------------
Provides:
1. NtpClient: RFC 5905 SNTP client measuring local clock offset AND its uncertainty.
2. HighPrecisionScheduler: Hybrid (async sleep + bounded spin-wait) scheduler for T0 target execution.

Honest precision note: the scheduler dispatches within a few microseconds of the LOCAL target,
but the true error versus the remote server clock is bounded by the NTP uncertainty
(round-trip / 2 of the best sample, typically 1 to 20 ms). Both numbers are reported.
"""

import asyncio
import logging
import socket
import struct
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from core.system import high_resolution_timer

logger = logging.getLogger("modules.retail.clock")

# NTP epoch starts at Jan 1 1900, Unix epoch starts at Jan 1 1970
NTP_EPOCH_DELTA_SEC = 2208988800
NTP_FRACTION_SCALE = 2**32
NTP_PACKET_SIZE = 48
NTP_PORT = 123
NTP_CLIENT_HEADER_BYTE = 0x23  # LI = 0, VN = 4, Mode = 3 (client)
NTP_MODE_SERVER = 4
NTP_LEAP_UNSYNCHRONIZED = 3
NTP_STRATUM_KISS_OF_DEATH = 0
NTP_MAX_ACCEPTED_RTT_MS = 1500.0

# [FEATURE: THREE_PHASE_WAIT] coarse sleep -> cooperative yield-spin -> bounded hard spin.
# Raison: measured on Windows (2026-10-01, 1ms timer active): asyncio.sleep overshoot is
#         p50 1.9 ms but p99 65 ms / max 86 ms under load. A fixed 4 ms spin window fired late.
# Attention: during the yield phase the loop keeps running other tasks (heartbeats) but one
#            core is busy for up to COARSE_SLEEP_MARGIN_MS; only the last HARD_SPIN_WINDOW_MS
#            blocks the loop. A task hogging the loop during the yield phase delays T0.
COARSE_SLEEP_MARGIN_MS = 100.0
HARD_SPIN_WINDOW_MS = 1.0
NS_PER_MS = 1_000_000
NS_PER_SEC = 1_000_000_000


class ClockSyncError(RuntimeError):
    """Raised when no NTP server gives a usable answer (fail-closed scheduling)."""


@dataclass
class NtpSyncResult:
    server: str
    offset_ms: float  # (Atomic Time - Local Time) in ms
    round_trip_ms: float  # Network RTT to NTP server
    stratum: int
    precision: float
    synced_at_local: float  # Local time.time() when sync completed

    @property
    def uncertainty_ms(self) -> float:
        """Maximum offset error for this sample: half the round trip (asymmetric path worst case)."""
        return self.round_trip_ms / 2.0


def _to_ntp_timestamp(unix_seconds: float) -> tuple[int, int]:
    ntp_seconds = unix_seconds + NTP_EPOCH_DELTA_SEC
    integer_part = int(ntp_seconds)
    fraction_part = int((ntp_seconds - integer_part) * NTP_FRACTION_SCALE)
    return integer_part, fraction_part


def _from_ntp_timestamp(integer_part: int, fraction_part: int) -> float:
    return integer_part - NTP_EPOCH_DELTA_SEC + fraction_part / float(NTP_FRACTION_SCALE)


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

    def __init__(self, timeout: float = 2.0, port: int = NTP_PORT):
        self.timeout = timeout
        self.port = port
        self.cached_offset_ms: float = 0.0
        self.cached_uncertainty_ms: Optional[float] = None
        self.last_sync_time: float = 0.0

    @property
    def is_synced(self) -> bool:
        return self.cached_uncertainty_ms is not None

    def query_server(self, host: str, port: Optional[int] = None) -> Optional[NtpSyncResult]:
        """
        Sends an SNTP query to the given server and calculates offset and delay.
        Returns None (and logs why) on any invalid or unusable answer.
        """
        port = port or self.port
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.settimeout(self.timeout)

        try:
            # [FEATURE: NTP_MONOTONIC_RTT] T4 is derived from T1 + monotonic elapsed time.
            # Raison: two time.time() reads can jump if the OS clock is adjusted mid-query.
            # Attention: the originate timestamp check rejects stale or spoofed replies.
            send_wall_time = time.time()
            send_perf_ns = time.perf_counter_ns()
            transmit_seconds, transmit_fraction = _to_ntp_timestamp(send_wall_time)
            request_packet = bytes([NTP_CLIENT_HEADER_BYTE]) + bytes(39) + struct.pack(
                "!II", transmit_seconds, transmit_fraction
            )
            udp_socket.sendto(request_packet, (host, port))

            response_packet, _ = udp_socket.recvfrom(1024)
            receive_perf_ns = time.perf_counter_ns()
            receive_wall_time = send_wall_time + (receive_perf_ns - send_perf_ns) / NS_PER_SEC

            if len(response_packet) < NTP_PACKET_SIZE:
                logger.warning("NTP %s: short packet (%d bytes)", host, len(response_packet))
                return None

            unpacked = struct.unpack("!BBBb11I", response_packet[:NTP_PACKET_SIZE])
            header_byte, stratum, precision = unpacked[0], unpacked[1], unpacked[3]
            leap_indicator = header_byte >> 6
            mode = header_byte & 0x7

            if mode != NTP_MODE_SERVER:
                logger.warning("NTP %s: unexpected mode %d", host, mode)
                return None
            if stratum == NTP_STRATUM_KISS_OF_DEATH or leap_indicator == NTP_LEAP_UNSYNCHRONIZED:
                logger.warning("NTP %s: server unsynchronized or rate-limiting (stratum=%d)", host, stratum)
                return None
            if (unpacked[9], unpacked[10]) != (transmit_seconds, transmit_fraction):
                logger.warning("NTP %s: originate timestamp mismatch, reply discarded", host)
                return None

            server_receive_time = _from_ntp_timestamp(unpacked[11], unpacked[12])
            server_transmit_time = _from_ntp_timestamp(unpacked[13], unpacked[14])

            round_trip_sec = (receive_wall_time - send_wall_time) - (server_transmit_time - server_receive_time)
            offset_sec = ((server_receive_time - send_wall_time) + (server_transmit_time - receive_wall_time)) / 2.0

            return NtpSyncResult(
                server=host,
                offset_ms=offset_sec * 1000.0,
                round_trip_ms=max(0.0, round_trip_sec * 1000.0),
                stratum=stratum,
                precision=float(precision),
                synced_at_local=receive_wall_time,
            )

        except OSError as error:
            logger.warning("NTP %s: query failed: %s", host, error)
            return None
        finally:
            udp_socket.close()

    def _process_results(self, results: List[NtpSyncResult]) -> Dict[str, Any]:
        if not results:
            return {"success": False, "error": "All NTP queries failed", "offset_ms": 0.0}

        reliable_results = [r for r in results if r.round_trip_ms < NTP_MAX_ACCEPTED_RTT_MS]
        if not reliable_results:
            return {"success": False, "error": "All NTP replies exceeded the RTT limit", "offset_ms": 0.0}

        # [FEATURE: NTP_MIN_DELAY_SELECTION] Keep the lowest-RTT sample, as the RFC 5905 clock filter does.
        # Raison: offset error is bounded by RTT / 2, so the fastest reply is the most trustworthy.
        #         The previous median mixed precise and imprecise samples.
        # Attention: median_offset_ms is still reported, for diagnostics and GUI compatibility.
        best_sample = min(reliable_results, key=lambda r: r.round_trip_ms)
        sorted_offsets = sorted(r.offset_ms for r in reliable_results)
        median_offset_ms = sorted_offsets[len(sorted_offsets) // 2]
        offset_spread_ms = sorted_offsets[-1] - sorted_offsets[0]

        self.cached_offset_ms = best_sample.offset_ms
        self.cached_uncertainty_ms = best_sample.uncertainty_ms
        self.last_sync_time = time.time()

        return {
            "success": True,
            "offset_ms": round(best_sample.offset_ms, 3),
            "uncertainty_ms": round(best_sample.uncertainty_ms, 3),
            "best_server": best_sample.server,
            "median_offset_ms": round(median_offset_ms, 3),
            "offset_spread_ms": round(offset_spread_ms, 3),
            "servers_responded": len(results),
            "reliable_servers": len(reliable_results),
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
        """Blocking sequential sync. Do NOT call from inside an event loop; use sync_async()."""
        servers = servers or self.DEFAULT_SERVERS
        results = [r for r in (self.query_server(s) for s in servers) if r is not None]
        return self._process_results(results)

    async def sync_async(self, servers: Optional[List[str]] = None) -> Dict[str, Any]:
        """Queries all servers in parallel worker threads, without blocking the event loop."""
        servers = servers or self.DEFAULT_SERVERS
        raw_results = await asyncio.gather(*(asyncio.to_thread(self.query_server, s) for s in servers))
        results = [r for r in raw_results if r is not None]
        return self._process_results(results)

    def get_atomic_time(self) -> float:
        """Returns the current true UTC time (in seconds) adjusted for clock offset."""
        return time.time() + (self.cached_offset_ms / 1000.0)


class HighPrecisionScheduler:
    """
    Schedules execution at exact atomic UTC timestamps.
    Coarse asyncio.sleep until COARSE_SLEEP_MARGIN_MS before target, then a cooperative
    yield-spin (asyncio.sleep(0)), then a hard spin for the last HARD_SPIN_WINDOW_MS.
    The event loop is blocked at most HARD_SPIN_WINDOW_MS per call.
    """

    def __init__(self, ntp_client: Optional[NtpClient] = None):
        self.ntp = ntp_client or NtpClient()

    async def wait_until_atomic_timestamp(
        self,
        target_atomic_timestamp_utc: float,
        latency_advance_ms: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Waits until the target timestamp minus latency_advance_ms.
        :param target_atomic_timestamp_utc: True UTC timestamp (Unix seconds) of the event.
        :param latency_advance_ms: Network lead time (e.g. RTT / 2) to fire before T0.
        :return: dispatch_error_us (local precision), clock_uncertainty_ms (true bound), fired_late.
        :raises ClockSyncError: if the clock was never synced and no NTP server answers.
        """
        if not self.ntp.is_synced:
            sync_result = await self.ntp.sync_async()
            if not sync_result["success"]:
                raise ClockSyncError(sync_result["error"])

        with high_resolution_timer():
            clock_offset_ns = int(self.ntp.cached_offset_ms * NS_PER_MS)
            advance_ns = int(latency_advance_ms * NS_PER_MS)
            target_local_ns = int(target_atomic_timestamp_utc * NS_PER_SEC) - clock_offset_ns - advance_ns

            coarse_margin_ns = int(COARSE_SLEEP_MARGIN_MS * NS_PER_MS)
            hard_spin_window_ns = int(HARD_SPIN_WINDOW_MS * NS_PER_MS)
            remaining_ns = target_local_ns - time.time_ns()
            if remaining_ns > coarse_margin_ns:
                await asyncio.sleep((remaining_ns - coarse_margin_ns) / NS_PER_SEC)

            # Translate the wall-clock target into the monotonic clock once, before the fine phases.
            wall_reference_ns = time.time_ns()
            perf_reference_ns = time.perf_counter_ns()
            perf_target_ns = perf_reference_ns + (target_local_ns - wall_reference_ns)

            while perf_target_ns - time.perf_counter_ns() > hard_spin_window_ns:
                await asyncio.sleep(0)
            while time.perf_counter_ns() < perf_target_ns:
                pass

            fired_at_perf_ns = time.perf_counter_ns()

        dispatch_error_us = (fired_at_perf_ns - perf_target_ns) / 1000.0
        fired_late = perf_target_ns < perf_reference_ns
        if fired_late:
            logger.warning("Scheduler fired %.3f ms after target", (perf_reference_ns - perf_target_ns) / NS_PER_MS)

        return {
            "dispatch_error_us": round(dispatch_error_us, 2),
            "clock_uncertainty_ms": round(self.ntp.cached_uncertainty_ms or 0.0, 3),
            "fired_late": fired_late,
            "offset_applied_ms": round(self.ntp.cached_offset_ms, 3),
            "lead_time_applied_ms": round(latency_advance_ms, 3),
        }
