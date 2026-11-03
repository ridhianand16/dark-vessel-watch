"""The tracker's consumer reads ais.enriched with read_committed (read_committed.py)."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import time
import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import faust
import pytest
from aiokafka import TopicPartition  # type: ignore[import-untyped]
from confluent_kafka import Producer
from faust.transport.drivers import aiokafka as faust_aiokafka
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from stream.faust_app import read_committed


def worker_consumer(brokers: str, scheme: str) -> Any:
    """The aiokafka consumer Faust's worker would read with, built the way the tracker
    builds it (app.py's broker_consumer). Call inside a running event loop."""
    app = faust.App(
        f"dvw-test-rc-{uuid.uuid4().hex[:8]}",
        broker=f"kafka://{brokers}",
        broker_consumer=f"{scheme}://{brokers}",
        web_enabled=False,
    )
    consumer, transport = app.consumer, app.transport
    assert isinstance(consumer, faust_aiokafka.Consumer) and isinstance(transport, faust_aiokafka.Transport)
    return consumer._new_consumer_thread()._create_worker_consumer(transport)


def test_worker_consumer_reads_committed() -> None:
    async def build() -> object:
        consumer = worker_consumer("localhost:1", read_committed.SCHEME)
        return consumer

    kafka_consumer = asyncio.run(build())
    # The value aiokafka hands to its fetcher in start().
    assert getattr(kafka_consumer, "_isolation_level") == "read_committed"


def test_aiokafka_passes_the_attribute_to_its_fetcher() -> None:
    """read_committed.py sets a private attribute; this fails if aiokafka stops using it."""
    consumer_cls = importlib.import_module("aiokafka").AIOKafkaConsumer
    assert "isolation_level=self._isolation_level" in inspect.getsource(consumer_cls.start)


def test_missing_attribute_is_refused() -> None:
    with pytest.raises(read_committed.ReadCommittedError, match="no _isolation_level"):
        read_committed.set_isolation_level(SimpleNamespace())


@pytest.mark.parametrize(
    "consumer",
    [
        SimpleNamespace(),  # no fetcher
        SimpleNamespace(_fetcher=SimpleNamespace()),  # fetcher without the attribute
        SimpleNamespace(_fetcher=SimpleNamespace(_isolation_level=0)),  # read_uncommitted
    ],
)
def test_started_consumer_not_reading_committed_is_refused(consumer: object) -> None:
    with pytest.raises(read_committed.ReadCommittedError):
        read_committed.check_isolation_level(consumer)


# --- against Redpanda (skipped when unreachable) --------------------------------


BROKERS = os.environ.get("REDPANDA_BROKERS", "localhost:19092")


@pytest.fixture
def topic_with_aborted_transaction() -> Iterator[str]:
    """A fresh one-partition topic holding an aborted transaction, then a committed one:
    what ais.enriched looks like after a Flink TaskManager failure and recovery."""
    admin = AdminClient({"bootstrap.servers": BROKERS, "socket.timeout.ms": 2000})
    try:
        admin.list_topics(timeout=3)
    except Exception as exc:  # KafkaException: broker not reachable
        pytest.skip(f"Redpanda not reachable at {BROKERS}: {exc}")
    topic = f"dvw-test-rc-{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(10)
    try:
        producer = Producer({"bootstrap.servers": BROKERS, "transactional.id": f"{topic}-tx"})
        producer.init_transactions(10)
        producer.begin_transaction()
        producer.produce(topic, key=b"219026147", value=b"aborted")
        producer.flush(10)  # written to the log, then aborted
        producer.abort_transaction(10)
        producer.begin_transaction()
        producer.produce(topic, key=b"219026147", value=b"committed")
        producer.commit_transaction(10)
        yield topic
    finally:
        admin.delete_topics([topic])[topic].result(10)


def read_all(topic: str, scheme: str) -> list[bytes]:
    """Every value the tracker's worker consumer (or a stock one) reads from the topic."""

    async def go() -> list[bytes]:
        consumer = worker_consumer(BROKERS, scheme)
        await consumer.start()
        try:
            if scheme == read_committed.SCHEME:
                read_committed.check_isolation_level(consumer)  # what on_start() checks
            tp = TopicPartition(topic, 0)
            consumer.assign([tp])
            await consumer.seek_to_beginning(tp)
            end = (await consumer.end_offsets([tp]))[tp]  # the last stable offset when read_committed
            values: list[bytes] = []
            deadline = time.monotonic() + 15
            while await consumer.position(tp) < end and time.monotonic() < deadline:
                batch = await consumer.getmany(tp, timeout_ms=500)
                values += [m.value for m in batch.get(tp, [])]
            return values
        finally:
            await consumer.stop()

    return asyncio.run(go())


def test_tracker_consumer_skips_aborted_transaction(topic_with_aborted_transaction: str) -> None:
    assert read_all(topic_with_aborted_transaction, read_committed.SCHEME) == [b"committed"]


def test_stock_consumer_would_see_the_aborted_record(topic_with_aborted_transaction: str) -> None:
    """The control: without the transport the same topic yields both records, so the
    test above can tell the two apart."""
    assert read_all(topic_with_aborted_transaction, "kafka") == [b"aborted", b"committed"]
