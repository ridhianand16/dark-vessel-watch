"""Consume one topic in batches, write each batch, then commit its offsets.

The delivery loop shared by every sink (the ClickHouse sinks, the vessels_current
sink). A subclass says how a record becomes a row (to_row) and how a batch of rows is
stored (write); this module owns batching, retries, offset commits and shutdown.

Delivery: at-least-once into an idempotent store. Offsets are committed synchronously,
and only after write() has returned. A crash between write and commit re-delivers the
batch, so write() must make a second copy harmless. Exit codes from run(): 0 after a
clean shutdown (SIGTERM/SIGINT, in-flight batch written and committed), 1 when writes
keep failing.
"""

from __future__ import annotations

import logging
import signal
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from types import FrameType
from typing import Generic, TypeVar

from confluent_kafka import Consumer, KafkaException, TopicPartition

R = TypeVar("R")


class WriteFailed(Exception):
    pass


@dataclass(frozen=True)
class BatchSettings:
    max_rows: int = 50_000
    max_wait_s: float = 1.0
    write_attempts: int = 6
    backoff_base_s: float = 0.5
    backoff_max_s: float = 10.0


def consumer_config(group_id: str, brokers: str) -> dict[str, str | int | bool]:
    return {
        "bootstrap.servers": brokers,
        "group.id": group_id,
        "client.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # Only read committed transactional writes (Flink exactly-once producers later).
        "isolation.level": "read_committed",
        # Bound librdkafka's prefetch buffer so the container stays under mem_limit.
        "queued.max.messages.kbytes": 16384,
    }


@dataclass
class Batch(Generic[R]):
    rows: list[R] = field(default_factory=list)
    next_offsets: dict[int, int] = field(default_factory=dict)  # partition -> last offset + 1
    records: int = 0
    skipped: int = 0  # records to_row returned None for; committed, not written

    def add(self, row: R | None, partition: int, offset: int) -> None:
        self.records += 1
        if row is None:
            self.skipped += 1
        else:
            self.rows.append(row)
        self.next_offsets[partition] = max(self.next_offsets.get(partition, 0), offset + 1)


class BatchSink(ABC, Generic[R]):
    def __init__(self, name: str, topic: str, consumer: Consumer, settings: BatchSettings) -> None:
        self.name = name
        self.topic = topic
        self.log = logging.getLogger(f"{name}-sink")
        self.consumer = consumer
        self.s = settings
        self.batch: Batch[R] = Batch()
        self.stopping = False
        self.failed = False

    @abstractmethod
    def to_row(self, value: bytes | None, partition: int, offset: int, kafka_ts_ms: int) -> R | None:
        """The row for one record, or None to skip it (its offset is still committed)."""

    @abstractmethod
    def write(self, rows: list[R]) -> dict[str, int]:
        """Store rows durably, or raise. Returns counters for the batch log line."""

    def request_stop(self, signum: int, _frame: FrameType | None) -> None:
        self.stop(f"signal {signal.Signals(signum).name}")

    def stop(self, reason: str) -> None:
        """Make run() return after the in-flight batch is written and committed."""
        self.log.info("%s: finishing the in-flight batch, then exiting", reason)
        self.stopping = True

    def run(self) -> int:
        self.consumer.subscribe([self.topic], on_assign=self._on_assign, on_revoke=self._on_revoke)
        try:
            while not self.stopping:
                self._collect()
                self.flush()
        except WriteFailed as e:
            self.log.error("%s; exiting without committing, the batch will be re-read on restart", e)
            self.failed = True  # keeps _on_revoke (called from close) from retrying the write
            return 1
        finally:
            self.consumer.close()
        self.log.info("stopped cleanly")
        return 0

    def _collect(self) -> None:
        """Fill self.batch until max_rows records, max_wait_s after the first record, or stop."""
        deadline: float | None = None
        while self.batch.records < self.s.max_rows and not self.stopping:
            timeout = self.s.max_wait_s if deadline is None else deadline - time.monotonic()
            if timeout <= 0:
                return
            msgs = self.consumer.consume(min(10_000, self.s.max_rows - self.batch.records), timeout)
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
                self.batch.add(self.to_row(m.value(), p, off, max(ts_ms, 0)), p, off)
                if deadline is None:
                    deadline = time.monotonic() + self.s.max_wait_s

    def flush(self) -> None:
        """Write the batch, then commit its offsets. No-op for an empty batch."""
        b = self.batch
        if not b.records:
            return
        t0 = time.monotonic()
        counters = self._write_with_retry(b.rows) if b.rows else {}
        t1 = time.monotonic()
        offsets = [TopicPartition(self.topic, p, o) for p, o in sorted(b.next_offsets.items())]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as e:
            # The rows are stored. If the commit is lost (e.g. during a rebalance) the
            # batch is re-delivered, and write() is idempotent.
            self.log.warning("offset commit failed after a successful write, will be re-read: %s", e)
        t2 = time.monotonic()
        self.log.info(
            "batch rows=%d partitions=%d skipped=%d %swrite_ms=%.0f commit_ms=%.0f",
            len(b.rows), len(b.next_offsets), b.skipped,
            "".join(f"{k}={v} " for k, v in counters.items()), (t1 - t0) * 1000, (t2 - t1) * 1000,
        )
        self.batch = Batch()

    def _write_with_retry(self, rows: list[R]) -> dict[str, int]:
        for attempt in range(1, self.s.write_attempts + 1):
            try:
                return self.write(rows)
            except Exception as e:  # driver, network and server errors all look different
                if attempt == self.s.write_attempts:
                    raise WriteFailed(f"write of {len(rows)} rows failed {attempt} times: {e}") from e
                delay = min(self.s.backoff_base_s * 2 ** (attempt - 1), self.s.backoff_max_s)
                self.log.warning("write attempt %d/%d failed (%s); retrying in %.1fs",
                                 attempt, self.s.write_attempts, e, delay)
                time.sleep(delay)
        raise AssertionError("unreachable")

    def _on_assign(self, _c: Consumer, partitions: list[TopicPartition]) -> None:
        self.log.info("assigned partitions %s", sorted(tp.partition for tp in partitions))

    def _on_revoke(self, _c: Consumer, partitions: list[TopicPartition]) -> None:
        # Commit what we hold before losing the partitions, so the next owner does
        # not start from older offsets. Not needed for correctness, only saves rework.
        if self.batch.records and not self.failed:
            self.flush()
        self.log.info("revoked partitions %s", sorted(tp.partition for tp in partitions))
