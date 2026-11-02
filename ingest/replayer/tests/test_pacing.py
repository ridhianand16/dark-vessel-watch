from __future__ import annotations

import pytest

from ingest.replayer.pacing import Pacer, due_offset_s


class FakeClock:
    """Deterministic clock: sleeping advances time exactly; work can be simulated with advance()."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.now += s


def make(speed: float, clock: FakeClock, min_sleep_s: float = 0.005) -> Pacer:
    return Pacer(speed, clock=clock, sleep=clock.sleep, min_sleep_s=min_sleep_s)


def test_due_offset_divides_event_gap_by_speed() -> None:
    assert due_offset_s(60_000, 0, 1) == 60.0
    assert due_offset_s(60_000, 0, 50) == pytest.approx(1.2)
    assert due_offset_s(1_000, 0, 100) == pytest.approx(0.01)


@pytest.mark.parametrize("speed", [1, 7, 50, 100])
def test_total_wall_time_is_event_span_over_speed(speed: float) -> None:
    clock = FakeClock()
    pacer = make(speed, clock)
    start = clock.now
    # one hour of 1-second ticks, 20 rows per tick (like DMA)
    for second in range(3600):
        for _ in range(20):
            pacer.wait_for(second * 1000)
    assert clock.now - start == pytest.approx(3599 / speed)


def test_rows_sharing_a_timestamp_cost_one_sleep() -> None:
    clock = FakeClock()
    pacer = make(50, clock)
    for second in range(100):
        for _ in range(1000):
            pacer.wait_for(second * 1000)
    # 100 distinct seconds -> 99 gaps, 1s / 50 = 20 ms each, so one sleep per gap
    assert pacer.sleep_calls == 99
    assert all(s == pytest.approx(0.02) for s in clock.sleeps)


def test_sub_threshold_gaps_are_batched_without_drift() -> None:
    clock = FakeClock()
    pacer = make(100, clock, min_sleep_s=0.005)
    start = clock.now
    n = 10_000
    for i in range(n):  # 100 ms apart at 100x = 1 ms each, below the 5 ms threshold
        pacer.wait_for(i * 100)
    expected = (n - 1) * 0.1 / 100
    assert pacer.sleep_calls <= n // 5  # roughly one sleep per 5 rows, not one per row
    assert all(s >= 0.005 for s in clock.sleeps)
    assert expected - (clock.now - start) < 0.005  # never more than one threshold behind


def test_falling_behind_does_not_sleep_and_does_not_repay() -> None:
    clock = FakeClock()
    pacer = make(10, clock)
    pacer.wait_for(0)
    clock.now += 5.0  # producer stalled for 5 s; events up to 50 s are now overdue
    for ts in range(1_000, 50_001, 1_000):
        pacer.wait_for(ts)
    assert pacer.sleep_calls == 0
    pacer.wait_for(60_000)  # due at anchor + 6 s, it is now anchor + 5 s
    assert clock.sleeps == [pytest.approx(1.0)]


def test_rows_without_timestamp_are_not_delayed() -> None:
    clock = FakeClock()
    pacer = make(1, clock)
    pacer.wait_for(None)
    pacer.wait_for(0)
    pacer.wait_for(None)
    assert clock.sleeps == []


@pytest.mark.parametrize("speed", [0, 0.5, 100.5, -1])
def test_speed_out_of_range_is_rejected(speed: float) -> None:
    with pytest.raises(ValueError):
        Pacer(speed)
