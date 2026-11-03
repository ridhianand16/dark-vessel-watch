"""run_together: one failing sink stops the others; clean stops exit 0."""

from __future__ import annotations

import threading
import time
from typing import Any, cast

from confluent_kafka import Consumer

from common.kafka_batch import BatchSettings, BatchSink
from common.sink_group import run_together


class FakeSink(BatchSink[int]):
    """run() loops until stopped, or returns fail_code / raises after `after` seconds."""

    def __init__(self, name: str, fail_code: int = 0, raises: bool = False, after: float = 0.05) -> None:
        super().__init__(name, f"topic.{name}", cast(Consumer, object()), BatchSettings())
        self.fail_code, self.raises, self.after = fail_code, raises, after
        self.stop_reasons: list[str] = []
        self.started = threading.Event()

    def to_row(self, value: bytes | None, partition: int, offset: int, kafka_ts_ms: int) -> int:
        return 0

    def write(self, rows: list[int]) -> dict[str, int]:
        return {}

    def stop(self, reason: str) -> None:
        self.stop_reasons.append(reason)
        super().stop(reason)

    def run(self) -> int:
        self.started.set()
        deadline = time.monotonic() + self.after
        while not self.stopping:
            if (self.fail_code or self.raises) and time.monotonic() > deadline:
                if self.raises:
                    raise RuntimeError("boom")
                return self.fail_code
            time.sleep(0.005)
        return 0


def run(*sinks: FakeSink) -> int:
    return run_together(cast(list[BatchSink[Any]], list(sinks)), install_signals=False)


def test_a_failing_sink_stops_the_others_and_exits_1() -> None:
    ok, bad = FakeSink("alerts"), FakeSink("tracks", fail_code=1)
    assert run(ok, bad) == 1
    assert ok.stopping and ok.stop_reasons == ["tracks sink failed (exit 1)"]


def test_a_crashing_sink_counts_as_a_failure() -> None:
    ok, bad = FakeSink("alerts"), FakeSink("tracks", raises=True)
    assert run(ok, bad) == 1
    assert ok.stopping


def test_stop_from_outside_exits_0() -> None:
    a, b = FakeSink("alerts"), FakeSink("tracks")
    result: list[int] = []
    t = threading.Thread(target=lambda: result.append(run(a, b)))
    t.start()
    a.started.wait(1)
    b.started.wait(1)
    a.stop("test")
    b.stop("test")
    t.join(5)
    assert result == [0]
