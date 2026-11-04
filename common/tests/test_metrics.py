"""common/metrics.py: off by default, and the sink loop's numbers reach /metrics when on."""

from __future__ import annotations

import socket
import urllib.request
from collections import Counter
from collections.abc import Iterator
from typing import Any, cast

import pytest
from confluent_kafka import Consumer, KafkaException
from prometheus_client import CollectorRegistry

from common import metrics
from common.kafka_batch import BatchSettings, BatchSink


@pytest.fixture
def registry() -> Iterator[CollectorRegistry]:
    metrics.disable()
    reg = CollectorRegistry()
    yield reg
    metrics.disable()


class FakeConsumer:
    def __init__(self, commit_fails: bool = False) -> None:
        self.commit_fails = commit_fails
        self.commits: list[Any] = []

    def commit(self, offsets: Any, asynchronous: bool) -> None:
        if self.commit_fails:
            raise KafkaException("rebalance")
        self.commits.append(offsets)


class FlakySink(BatchSink[int]):
    """Writes fail `fail_first` times, then succeed; every 3rd record has no row."""

    def __init__(self, consumer: FakeConsumer, fail_first: int = 0) -> None:
        super().__init__("raw", "ais.raw", cast(Consumer, consumer),
                         BatchSettings(backoff_base_s=0.0, backoff_max_s=0.0))
        self.fail_first = fail_first

    def to_row(self, value: bytes | None, partition: int, offset: int, kafka_ts_ms: int) -> int | None:
        return None if offset % 3 == 2 else offset

    def write(self, rows: list[int]) -> dict[str, int]:
        if self.fail_first:
            self.fail_first -= 1
            raise ConnectionError("clickhouse down")
        return {"decode_errors": 1}


def fill(sink: FlakySink, n: int) -> None:
    for off in range(n):
        sink.batch.add(sink.to_row(b"x", 0, off, 0), 0, off)


def value(reg: CollectorRegistry, name: str, **labels: str) -> float:
    v = reg.get_sample_value(name, labels)
    assert v is not None, f"{name}{labels} not exported"
    return v


def test_off_without_metrics_port(registry: CollectorRegistry) -> None:
    assert metrics.serve_from_env({}) is None
    assert metrics.serve_from_env({"METRICS_PORT": " "}) is None
    assert not metrics.enabled()
    sink = FlakySink(FakeConsumer())
    fill(sink, 6)
    sink.flush()  # hooks are no-ops: nothing raises, nothing registered
    assert list(registry.collect()) == []


def test_flush_counts_records_rows_and_timings(registry: CollectorRegistry) -> None:
    metrics.enable(registry)
    sink = FlakySink(FakeConsumer())
    assert value(registry, "dvw_sink_records_total", sink="raw") == 0  # series exist before traffic
    fill(sink, 6)
    sink.flush()
    assert value(registry, "dvw_sink_records_total", sink="raw") == 6
    assert value(registry, "dvw_sink_rows_total", sink="raw") == 4
    assert value(registry, "dvw_sink_skipped_total", sink="raw") == 2
    assert value(registry, "dvw_sink_write_results_total", sink="raw", result="decode_errors") == 1
    assert value(registry, "dvw_sink_write_seconds_count", sink="raw") == 1
    assert value(registry, "dvw_sink_commit_seconds_count", sink="raw") == 1
    assert value(registry, "dvw_sink_last_commit_timestamp_seconds", sink="raw") > 0


def test_retries_and_failed_commits_are_counted(registry: CollectorRegistry) -> None:
    metrics.enable(registry)
    sink = FlakySink(FakeConsumer(commit_fails=True), fail_first=2)
    fill(sink, 3)
    sink.flush()
    assert value(registry, "dvw_sink_write_retries_total", sink="raw") == 2
    assert value(registry, "dvw_sink_commit_failures_total", sink="raw") == 1
    assert value(registry, "dvw_sink_rows_total", sink="raw") == 2  # the rows were stored


def test_exported_counter_follows_the_live_counter(registry: CollectorRegistry) -> None:
    metrics.enable(registry)
    counts: Counter[str] = Counter()
    metrics.export_counts("dvw_tracker_events", "Tracker outcomes", "event", counts)
    counts.update({"read": 10, "leak_duplicate": 0})
    assert value(registry, "dvw_tracker_events_total", event="read") == 10
    counts["read"] += 5
    assert value(registry, "dvw_tracker_events_total", event="read") == 15
    assert value(registry, "dvw_tracker_events_total", event="leak_duplicate") == 0
    metrics.tracker_batch(0.02, 250)
    assert value(registry, "dvw_tracker_batch_records_count") == 1


def test_serves_metrics_over_http(registry: CollectorRegistry) -> None:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    assert metrics.serve_from_env({"METRICS_PORT": str(port)}, registry) == port
    FlakySink(FakeConsumer())
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
        body = r.read().decode()
    assert 'dvw_sink_records_total{sink="raw"} 0.0' in body
