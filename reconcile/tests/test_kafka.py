from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from reconcile import kafka
from reconcile.run_check import RunError


class SilentConsumer:
    """One partition that never reaches its end: poll() returns nothing, forever."""

    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self.closed = False

    def list_topics(self, topic: str, timeout: float) -> Any:
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions={0: None})})

    def offsets_for_times(self, wanted: list[Any], timeout: float) -> list[Any]:
        return [SimpleNamespace(partition=0, offset=0)]

    def assign(self, tps: list[Any]) -> None:
        pass

    def poll(self, timeout: float) -> None:
        return None

    def close(self) -> None:
        self.closed = True


def test_a_read_that_never_reaches_the_end_gives_up(monkeypatch: pytest.MonkeyPatch) -> None:
    consumers: list[SilentConsumer] = []

    def make(conf: dict[str, Any]) -> SilentConsumer:
        consumers.append(SilentConsumer(conf))
        return consumers[-1]

    monkeypatch.setattr(kafka, "Consumer", make)
    with pytest.raises(RunError, match=r"did not reach the end of partitions \[0\]"):
        list(kafka.read("b:9092", "ais.tracks", 0, "g", timeout_s=0.05))
    assert consumers[0].closed
    assert consumers[0].conf["isolation.level"] == "read_committed"
