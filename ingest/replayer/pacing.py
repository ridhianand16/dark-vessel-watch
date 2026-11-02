"""Event-time pacing for the replayer.

Every send is scheduled against a fixed anchor (first event_ts, wall clock at that
moment): due = anchor_wall + (event_ts - anchor_event) / speed. Because the target is
absolute rather than "sleep the gap since the last row", small errors never
accumulate into drift.

Sleeps are batched: a row is sent immediately unless it is due at least
`min_sleep_s` in the future. Rows sharing a timestamp, or gaps too small to be worth
a syscall, go out back to back, and the next real sleep absorbs the accumulated
delay. At 100x with 1-second DMA timestamps that is at most one sleep per distinct
second of data (86,400 per day), not one per row.

If sending falls behind schedule (slow broker, backpressure) the replayer does not
sleep until it has caught up. It never tries to "repay" the lag by slowing down later.
"""

from __future__ import annotations

import time
from collections.abc import Callable

MIN_SPEED = 1.0
MAX_SPEED = 100.0
DEFAULT_MIN_SLEEP_S = 0.005


def due_offset_s(event_ts_ms: int, anchor_event_ms: int, speed: float) -> float:
    """Seconds after the anchor's wall-clock time at which this event is due."""
    return (event_ts_ms - anchor_event_ms) / 1000.0 / speed


class Pacer:
    def __init__(
        self,
        speed: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        min_sleep_s: float = DEFAULT_MIN_SLEEP_S,
    ) -> None:
        if not MIN_SPEED <= speed <= MAX_SPEED:
            raise ValueError(f"speed must be in [{MIN_SPEED:g}, {MAX_SPEED:g}], got {speed}")
        self.speed = speed
        self._clock = clock
        self._sleep = sleep
        self._min_sleep_s = min_sleep_s
        self._anchor_event_ms: int | None = None
        self._anchor_wall = 0.0
        self.sleep_calls = 0
        self.slept_s = 0.0

    def wait_for(self, event_ts_ms: int | None) -> None:
        """Block until the event is due. Rows without a timestamp are never delayed."""
        if event_ts_ms is None:
            return
        if self._anchor_event_ms is None:
            self._anchor_event_ms = event_ts_ms
            self._anchor_wall = self._clock()
            return
        due = self._anchor_wall + due_offset_s(event_ts_ms, self._anchor_event_ms, self.speed)
        delay = due - self._clock()
        if delay >= self._min_sleep_s:
            self._sleep(delay)
            self.sleep_calls += 1
            self.slept_s += delay
