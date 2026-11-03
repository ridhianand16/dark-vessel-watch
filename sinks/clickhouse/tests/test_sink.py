"""Commit ordering with fake Kafka and ClickHouse clients."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from typing import Any, cast

import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer, TopicPartition

from common.kafka_batch import BatchSettings, WriteFailed
from sinks.clickhouse.sink import ClickHouseSink, SinkSpec
from sinks.clickhouse.specs import SPECS


class FakeMessage:
    def __init__(self, partition: int, offset: int) -> None:
        self._p, self._o = partition, offset

    def error(self) -> None:
        return None

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o

    def timestamp(self) -> tuple[int, int]:
        return 1, 1_761_609_601_000

    def value(self) -> bytes:
        return json.dumps({"schema_version": 1, "run_id": "r1", "mmsi": 1}).encode()


class FakeConsumer:
    def __init__(self, batches: list[list[FakeMessage]]) -> None:
        self.batches = batches
        self.commits: list[list[tuple[int, int]]] = []
        self.events: list[str] = []
        # The real loop polls until a record arrives or a stop is requested, so the
        # fake requests a stop once it runs dry (as SIGTERM would).
        self.on_drained: Callable[[], None] = lambda: None

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        if self.batches:
            return self.batches.pop(0)
        self.on_drained()
        return []

    def commit(self, offsets: list[TopicPartition], asynchronous: bool) -> None:
        assert asynchronous is False
        self.events.append("commit")
        self.commits.append([(tp.partition, tp.offset) for tp in offsets])


class FakeClient:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.inserted: list[int] = []
        self.tables: list[str] = []
        self.last_rows: list[list[object]] = []
        self.consumer: FakeConsumer | None = None

    def insert(self, table: str, rows: list[list[object]], **kwargs: Any) -> None:
        if self.consumer is not None:
            self.consumer.events.append("insert")
        if self.failures:
            self.failures -= 1
            raise ConnectionError("clickhouse down")
        self.inserted.append(len(rows))
        self.tables.append(table)
        self.last_rows = rows


def make(
    batches: list[list[FakeMessage]], failures: int, attempts: int = 3, name: str = "raw"
) -> tuple[ClickHouseSink, FakeConsumer, FakeClient]:
    consumer, client = FakeConsumer(batches), FakeClient(failures)
    client.consumer = consumer
    s = BatchSettings(max_rows=10, max_wait_s=0.01, write_attempts=attempts, backoff_base_s=0.0)
    spec = replace(SPECS[name], settings=s)
    sink = ClickHouseSink(spec, cast(Consumer, consumer), cast(Client, client))
    consumer.on_drained = lambda: setattr(sink, "stopping", True)
    return sink, consumer, client


def test_commits_next_offset_per_partition_after_insert() -> None:
    sink, consumer, client = make([[FakeMessage(0, 5), FakeMessage(2, 9), FakeMessage(0, 6)]], failures=0)
    sink._collect()
    sink.flush()
    assert client.inserted == [3]
    assert consumer.events == ["insert", "commit"]
    assert consumer.commits == [[(0, 7), (2, 10)]]


def test_retries_then_commits_once() -> None:
    sink, consumer, client = make([[FakeMessage(1, 0)]], failures=2)
    sink._collect()
    sink.flush()
    assert consumer.events == ["insert", "insert", "insert", "commit"]


def test_never_commits_when_inserts_keep_failing() -> None:
    sink, consumer, _ = make([[FakeMessage(1, 0)]], failures=99)
    sink._collect()
    with pytest.raises(WriteFailed):
        sink.flush()
    assert consumer.commits == []
    assert len(sink.batch.rows) == 1  # left in place; the process exits and re-reads


def test_empty_batch_does_nothing() -> None:
    sink, consumer, client = make([], failures=0)
    sink._collect()
    sink.flush()
    assert consumer.events == [] and client.inserted == []


def test_spec_picks_topic_and_table() -> None:
    sink, consumer, client = make([[FakeMessage(4, 1)]], failures=0, name="alerts")
    sink._collect()
    sink.flush()
    assert client.tables == ["dvw.alerts"]
    assert consumer.commits == [[(4, 2)]]
    # FakeMessage's value is not an alert: stored anyway, under a per-record id.
    assert client.last_rows[0][sink.spec.columns.index("alert_id")] == "invalid:4:1"


def test_spec_requires_decode_error_column() -> None:
    with pytest.raises(ValueError, match="decode_error"):
        replace(SPECS["raw"], columns=("run_id",))


@pytest.mark.parametrize("spec", SPECS.values(), ids=SPECS.keys())
def test_every_mapper_fills_every_column(spec: SinkSpec) -> None:
    for value in (None, b"", b"\xff", b"[]", b'{"schema_version": 1}', FakeMessage(0, 0).value()):
        row = spec.to_row(value, 0, 0, 0)
        assert len(row) == len(spec.columns)
