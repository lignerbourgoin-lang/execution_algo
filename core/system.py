"""
Low-Level OS & Kernel Tuning for Execution Algorithms
-----------------------------------------------------
Maximizes execution predictability by:
1. Elevating process priority class to HIGH_PRIORITY_CLASS (Windows) or nice level (Linux).
2. Forcing kernel interrupt timer to 1ms (timeBeginPeriod) to eliminate Windows sleep jitter.
"""

import os
import sys

_timer_period_active = False


def boost_process_performance() -> bool:
    """
    Elevates process priority and timer resolution to eliminate OS-level scheduling lag.
    """
    global _timer_period_active
    success = False

    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.windll.kernel32
            kernel32.GetCurrentProcess.restype = wintypes.HANDLE
            kernel32.SetPriorityClass.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            kernel32.SetPriorityClass.restype = wintypes.BOOL

            # HIGH_PRIORITY_CLASS = 0x00000080
            handle = kernel32.GetCurrentProcess()
            res_priority = kernel32.SetPriorityClass(handle, 0x00000080)

            # Request 1ms kernel timer tick resolution
            winmm = ctypes.windll.winmm
            winmm.timeBeginPeriod(1)
            _timer_period_active = True
            success = bool(res_priority)
        except Exception:
            pass

    elif hasattr(os, "nice"):
        try:
            os.nice(-5)
            success = True
        except Exception:
            pass

    return success


def restore_process_performance():
    """Restores default system timer resolution upon shutdown."""
    global _timer_period_active
    if sys.platform == "win32" and _timer_period_active:
        try:
            import ctypes
            ctypes.windll.winmm.timeEndPeriod(1)
            _timer_period_active = False
        except Exception:
            pass
