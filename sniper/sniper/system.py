from __future__ import annotations

import gc
import sys
from contextlib import contextmanager


def pin_current_thread(core_id: int = 0) -> None:
    if sys.platform == "win32":
        import ctypes
        mask = 1 << core_id
        ctypes.windll.kernel32.SetThreadAffinityMask(-1, mask)
    else:
        try:
            import os
            os.sched_setaffinity(0, {core_id})
        except (AttributeError, OSError):
            pass


@contextmanager
def win_timer_1ms():
    end = None
    if sys.platform == "win32":
        import ctypes
        winmm = ctypes.WinDLL("winmm")
        winmm.timeBeginPeriod(1)
        end = lambda: winmm.timeEndPeriod(1)
    try:
        yield
    finally:
        if end:
            end()


@contextmanager
def freeze_gc():
    gc.disable()
    try:
        yield
    finally:
        gc.enable()
        gc.collect()
