from __future__ import annotations

import time
from statistics import median


class Clock:
    def __init__(self) -> None:
        self.offset = 0.0
        self.calibrate()

    def calibrate(self) -> None:
        offsets: list[float] = []
        try:
            import ntplib
        except ImportError:
            self.offset = 0.0
            return
        client = ntplib.NTPClient()
        for server in ("time.google.com", "time.cloudflare.com", "pool.ntp.org"):
            try:
                offsets.append(float(client.request(server, version=3, timeout=1).offset))
            except Exception:
                continue
        self.offset = median(offsets) if offsets else 0.0

    def now(self) -> float:
        return time.time() + self.offset

    def sleep_until(self, target_ts: float) -> None:
        while True:
            remaining = target_ts - self.now()
            if remaining <= 0:
                return
            if remaining > 0.1:
                time.sleep(remaining - 0.05)
            elif remaining > 0.001:
                time.sleep(0.0004)
            else:
                while self.now() < target_ts:
                    pass
                return
