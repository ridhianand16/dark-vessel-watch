"""Commit ordering with fake Kafka and ClickHouse clients."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, cast

import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer, TopicPartition

from sinks.clickhouse_raw.sink import InsertFailed, RawSink, Settings


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
        self.consumer: FakeConsumer | None = None

    def insert(self, table: str, rows: list[list[object]], **kwargs: Any) -> None:
        if self.consumer is not None:
            self.consumer.events.append("insert")
        if self.failures:
            self.failures -= 1
            raise ConnectionError("clickhouse down")
        self.inserted.append(len(rows))


def make(batches: list[list[FakeMessage]], failures: int, attempts: int = 3) -> tuple[RawSink, FakeConsumer, FakeClient]:
    consumer, client = FakeConsumer(batches), FakeClient(failures)
    client.consumer = consumer
    s = Settings(max_rows=10, max_wait_s=0.01, insert_attempts=attempts, backoff_base_s=0.0)
    sink = RawSink(cast(Consumer, consumer), cast(Client, client), s)
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
    with pytest.raises(InsertFailed):
        sink.flush()
    assert consumer.commits == []
    assert len(sink.batch.rows) == 1  # left in place; the process exits and re-reads


def test_empty_batch_does_nothing() -> None:
    sink, consumer, client = make([], failures=0)
    sink._collect()
    sink.flush()
    assert consumer.events == [] and client.inserted == []
