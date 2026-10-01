"""
Low-Level OS & Kernel Tuning for Execution Algorithms
-----------------------------------------------------
Maximizes execution predictability by:
1. Elevating process priority class to HIGH_PRIORITY_CLASS (Windows) or nice level (Linux).
2. Forcing kernel interrupt timer to 1ms (timeBeginPeriod) to eliminate Windows sleep jitter.
"""

import contextlib
import logging
import os
import sys
import threading
from typing import Iterator

logger = logging.getLogger("core.system")

WINDOWS_HIGH_PRIORITY_CLASS = 0x00000080
WINDOWS_TIMER_RESOLUTION_MS = 1
WINDOWS_TIMERR_NOERROR = 0
UNIX_NICE_INCREMENT = -5

# [FEATURE: TIMER_RESOLUTION_REFCOUNT] One shared reference counter for timeBeginPeriod.
# Raison: run.py, the GUI and the T0 scheduler all requested 1ms resolution independently,
#         and the scheduler's timeEndPeriod cancelled the process-wide boost mid-run.
# Attention: every acquire_timer_resolution() MUST be paired with release_timer_resolution();
#            use the high_resolution_timer() context manager whenever possible.
_timer_lock = threading.Lock()
_timer_refcount = 0


def acquire_timer_resolution() -> bool:
    """Requests 1ms kernel timer resolution (Windows only). Returns True if active."""
    global _timer_refcount
    if sys.platform != "win32":
        return False
    with _timer_lock:
        if _timer_refcount == 0:
            try:
                import ctypes

                result_code = ctypes.windll.winmm.timeBeginPeriod(WINDOWS_TIMER_RESOLUTION_MS)
            except Exception as error:
                logger.warning("timeBeginPeriod unavailable: %s", error)
                return False
            if result_code != WINDOWS_TIMERR_NOERROR:
                logger.warning("timeBeginPeriod refused with code %s", result_code)
                return False
        _timer_refcount += 1
        return True


def release_timer_resolution() -> None:
    """Releases one reference; restores default resolution when the last one is released."""
    global _timer_refcount
    if sys.platform != "win32":
        return
    with _timer_lock:
        if _timer_refcount == 0:
            return
        _timer_refcount -= 1
        if _timer_refcount == 0:
            try:
                import ctypes

                ctypes.windll.winmm.timeEndPeriod(WINDOWS_TIMER_RESOLUTION_MS)
            except Exception as error:
                logger.warning("timeEndPeriod failed: %s", error)


@contextlib.contextmanager
def high_resolution_timer() -> Iterator[bool]:
    """Context manager holding 1ms timer resolution for the duration of the block."""
    is_active = acquire_timer_resolution()
    try:
        yield is_active
    finally:
        if is_active:
            release_timer_resolution()


def boost_process_performance() -> bool:
    """
    Elevates process priority and timer resolution to reduce OS-level scheduling lag.
    Returns True only if the priority elevation succeeded. Failures are logged, never silent.
    """
    priority_elevated = False

    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.windll.kernel32
            kernel32.GetCurrentProcess.restype = wintypes.HANDLE
            kernel32.SetPriorityClass.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            kernel32.SetPriorityClass.restype = wintypes.BOOL

            process_handle = kernel32.GetCurrentProcess()
            priority_elevated = bool(kernel32.SetPriorityClass(process_handle, WINDOWS_HIGH_PRIORITY_CLASS))
        except Exception as error:
            logger.warning("SetPriorityClass failed: %s", error)
        acquire_timer_resolution()

    elif hasattr(os, "nice"):
        try:
            os.nice(UNIX_NICE_INCREMENT)
            priority_elevated = True
        except PermissionError:
            logger.warning("os.nice(%s) requires root privileges; priority unchanged", UNIX_NICE_INCREMENT)

    if not priority_elevated:
        logger.warning("Process priority could not be elevated")
    return priority_elevated


def restore_process_performance() -> None:
    """Releases the timer resolution acquired by boost_process_performance()."""
    release_timer_resolution()
