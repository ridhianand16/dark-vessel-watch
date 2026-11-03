"""Consume one topic and insert every record into one ClickHouse table.

    uv run python -m sinks.clickhouse raw      # ais.raw -> dvw.ais_raw (Compose: raw-sink)
    uv run python -m sinks.clickhouse alerts   # ais.alerts -> dvw.alerts (Compose: alerts-sink)

What differs between instances is a SinkSpec (specs.py): topic, consumer group, table,
columns and the row mapper. The delivery logic here is shared.

Delivery: at-least-once into an idempotent table. Offsets are committed synchronously,
and only after the batch insert has succeeded. A crash between insert and commit
re-delivers that batch; the table's ReplacingMergeTree key collapses the copies
(raw: Kafka coordinates, alerts: alert_id). Exit codes: 0 after a clean shutdown
(SIGTERM/SIGINT, in-flight batch inserted and committed), 1 when inserts keep failing
or the table is missing, 2 for an unknown sink name (argparse).
"""

from __future__ import annotations

import logging
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from types import FrameType

from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer, KafkaException, TopicPartition

RowMapper = Callable[[bytes | None, int, int, int], list[object]]
"""(value, partition, offset, kafka_ts_ms) -> the row's values in SinkSpec.columns order.
Must never raise: a value it cannot use still becomes a row, with a decode_error."""


class InsertFailed(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    max_rows: int = 50_000
    max_wait_s: float = 1.0
    insert_attempts: int = 6
    backoff_base_s: float = 0.5
    backoff_max_s: float = 10.0


@dataclass(frozen=True)
class SinkSpec:
    """One topic into one table: everything that differs between sink instances."""

    name: str
    topic: str
    group_id: str
    table: str
    columns: tuple[str, ...]
    to_row: RowMapper
    settings: Settings = Settings()

    def __post_init__(self) -> None:
        if "decode_error" not in self.columns:
            raise ValueError(f"{self.name}: columns must include decode_error")

    @property
    def decode_error_index(self) -> int:
        return self.columns.index("decode_error")


def consumer_config(spec: SinkSpec, brokers: str) -> dict[str, str | int | bool]:
    return {
        "bootstrap.servers": brokers,
        "group.id": spec.group_id,
        "client.id": spec.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # Only read committed transactional writes (Flink exactly-once producers later).
        "isolation.level": "read_committed",
        # Bound librdkafka's prefetch buffer so the container stays under mem_limit.
        "queued.max.messages.kbytes": 16384,
    }


@dataclass
class Batch:
    decode_error_index: int
    rows: list[list[object]] = field(default_factory=list)
    next_offsets: dict[int, int] = field(default_factory=dict)  # partition -> last offset + 1
    decode_errors: int = 0

    def add(self, row: list[object], partition: int, offset: int) -> None:
        self.rows.append(row)
        self.next_offsets[partition] = max(self.next_offsets.get(partition, 0), offset + 1)
        if row[self.decode_error_index]:
            self.decode_errors += 1


class ClickHouseSink:
    def __init__(self, spec: SinkSpec, consumer: Consumer, client: Client) -> None:
        self.spec = spec
        self.log = logging.getLogger(f"{spec.name}-sink")
        self.consumer = consumer
        self.client = client
        self.s = spec.settings
        self.batch = self._new_batch()
        self.stopping = False
        self.failed = False

    def request_stop(self, signum: int, _frame: FrameType | None) -> None:
        self.log.info("signal %s: finishing the in-flight batch, then exiting", signal.Signals(signum).name)
        self.stopping = True

    def _new_batch(self) -> Batch:
        return Batch(self.spec.decode_error_index)

    def run(self) -> int:
        self.consumer.subscribe([self.spec.topic], on_assign=self._on_assign, on_revoke=self._on_revoke)
        try:
            while not self.stopping:
                self._collect()
                self.flush()
        except InsertFailed as e:
            self.log.error("%s; exiting without committing, the batch will be re-read on restart", e)
            self.failed = True  # keeps _on_revoke (called from close) from retrying the insert
            return 1
        finally:
            self.consumer.close()
        self.log.info("stopped cleanly")
        return 0

    def _collect(self) -> None:
        """Fill self.batch until max_rows, max_wait_s after the first record, or stop."""
        deadline: float | None = None
        while len(self.batch.rows) < self.s.max_rows and not self.stopping:
            timeout = self.s.max_wait_s if deadline is None else deadline - time.monotonic()
            if timeout <= 0:
                return
            msgs = self.consumer.consume(min(10_000, self.s.max_rows - len(self.batch.rows)), timeout)
            for m in msgs:
                err = m.error()
                if err is not None:
                    if err.fatal():
                        raise KafkaException(err)
                    self.log.warning("consumer error (not a record, nothing skipped): %s", err)
                    continue
                p, off = m.partition(), m.offset()
                if p is None or off is None:  # only error events lack these
                    continue
                _, ts_ms = m.timestamp()
                # ts_ms is -1 when the broker gave no timestamp; Redpanda always does.
                self.batch.add(self.spec.to_row(m.value(), p, off, max(ts_ms, 0)), p, off)
                if deadline is None:
                    deadline = time.monotonic() + self.s.max_wait_s

    def flush(self) -> None:
        """Insert the batch, then commit its offsets. No-op for an empty batch."""
        b = self.batch
        if not b.rows:
            return
        t0 = time.monotonic()
        self._insert_with_retry(b.rows)
        t1 = time.monotonic()
        offsets = [TopicPartition(self.spec.topic, p, o) for p, o in sorted(b.next_offsets.items())]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as e:
            # The rows are stored. If the commit is lost (e.g. during a rebalance) the
            # batch is re-delivered and collapses on its Kafka coordinates.
            self.log.warning("offset commit failed after a successful insert, will be re-read: %s", e)
        t2 = time.monotonic()
        self.log.info(
            "batch rows=%d partitions=%d decode_errors=%d insert_ms=%.0f commit_ms=%.0f",
            len(b.rows), len(b.next_offsets), b.decode_errors, (t1 - t0) * 1000, (t2 - t1) * 1000,
        )
        self.batch = self._new_batch()

    def _insert_with_retry(self, rows: list[list[object]]) -> None:
        for attempt in range(1, self.s.insert_attempts + 1):
            try:
                self.client.insert(
                    self.spec.table, rows, column_names=list(self.spec.columns), settings={"async_insert": 0}
                )
                return
            except Exception as e:  # driver, network and server errors all look different
                if attempt == self.s.insert_attempts:
                    raise InsertFailed(f"insert of {len(rows)} rows failed {attempt} times: {e}") from e
                delay = min(self.s.backoff_base_s * 2 ** (attempt - 1), self.s.backoff_max_s)
                self.log.warning("insert attempt %d/%d failed (%s); retrying in %.1fs",
                                 attempt, self.s.insert_attempts, e, delay)
                time.sleep(delay)

    def _on_assign(self, _c: Consumer, partitions: list[TopicPartition]) -> None:
        self.log.info("assigned partitions %s", sorted(tp.partition for tp in partitions))

    def _on_revoke(self, _c: Consumer, partitions: list[TopicPartition]) -> None:
        # Commit what we hold before losing the partitions, so the next owner does
        # not start from older offsets. Not needed for correctness, only saves rework.
        if self.batch.rows and not self.failed:
            self.flush()
        self.log.info("revoked partitions %s", sorted(tp.partition for tp in partitions))
