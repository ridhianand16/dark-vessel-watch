"""Kafka helpers shared by the reconcile tools: reading a topic from a run's start time,
and how far a consumer group has committed."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from confluent_kafka import OFFSET_END, Consumer, KafkaError, KafkaException, TopicPartition

from reconcile.run_check import RunError
from stream.faust_app.config import ENRICHED_TOPIC, RAW_TOPIC

SEEK_MARGIN_MS = 60_000
TRACKER_GROUP = "dvw-tracker"
FLINK_GROUP = "dvw-flink-enrichment"  # EnrichmentJob.GROUP_ID
ALERTS_SINK_GROUP = "dvw-alerts-sink"


def run_started_ms(path: Path) -> int:
    doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return int(datetime.fromisoformat(doc["started_at"]).timestamp() * 1000)


@dataclass(frozen=True)
class Record:
    value: dict[str, Any]
    partition: int
    offset: int
    timestamp_ms: int


def read(brokers: str, topic: str, since_ms: int, group: str, timeout_s: float | None = None) -> Iterator[Record]:
    """Every committed record of `topic` written at or after since_ms, up to the current end.

    With read_committed, the end of a partition is its last stable offset: records of a
    transaction Flink has not committed yet are neither returned nor waited for. Topics
    without transactions read the same either way. `group` is never committed.
    timeout_s bounds the whole read; None waits until every partition reaches its end.
    """
    c = Consumer({
        "bootstrap.servers": brokers,
        "group.id": group,
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "enable.partition.eof": True,
        "auto.offset.reset": "earliest",
    })
    try:
        md = c.list_topics(topic, timeout=10)
        if topic not in md.topics or md.topics[topic].error is not None:
            raise RunError(f"topic {topic} not found")
        wanted = [TopicPartition(topic, p, since_ms) for p in md.topics[topic].partitions]
        start = c.offsets_for_times(wanted, timeout=10)
        # -1: nothing written since since_ms, so start at the end.
        c.assign([TopicPartition(topic, tp.partition, tp.offset if tp.offset >= 0 else OFFSET_END) for tp in start])
        remaining = {tp.partition for tp in start}
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while remaining:
            if deadline is not None and time.monotonic() > deadline:
                raise RunError(f"{topic}: did not reach the end of partitions {sorted(remaining)} in {timeout_s:.0f} s")
            msg = c.poll(1.0)
            if msg is None:
                continue
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    remaining.discard(msg.partition())
                    continue
                raise KafkaException(err)
            value, partition, offset = msg.value(), msg.partition(), msg.offset()
            if value is not None and partition is not None and offset is not None:
                yield Record(json.loads(value), partition, offset, msg.timestamp()[1])
    finally:
        c.close()


def read_topic(brokers: str, topic: str, since_ms: int, timeout_s: float = 120.0) -> Iterator[dict[str, Any]]:
    """The values of read(), within timeout_s."""
    for r in read(brokers, topic, since_ms, f"dvw-flink-check-{uuid.uuid4().hex[:8]}", timeout_s):
        yield r.value


def run_last_offsets(brokers: str, topic: str, run_id: str, since_ms: int) -> dict[int, int]:
    """Offset of the run's last record per partition of `topic`, read committed from
    since_ms to the current end. Partitions without the run's records are left out."""
    last: dict[int, int] = {}
    for r in read(brokers, topic, since_ms, f"dvw-score-{uuid.uuid4().hex[:8]}"):
        if r.value.get("run_id") == run_id:
            last[r.partition] = r.offset
    return last


def committed(brokers: str, group: str, tps: list[TopicPartition]) -> list[TopicPartition]:
    """A group's committed offsets (the next offset to read). Does not join the group."""
    c = Consumer({"bootstrap.servers": brokers, "group.id": group, "enable.auto.commit": False})
    try:
        return list(c.committed(tps, timeout=10))
    finally:
        c.close()


def lagging(brokers: str, group: str, topic: str, last: dict[int, int]) -> list[str]:
    """Partitions where `group` has not committed past offset last[p]."""
    out = []
    for tp in committed(brokers, group, [TopicPartition(topic, p) for p in sorted(last)]):
        left = last[tp.partition] + 1 - max(tp.offset, 0)
        if left > 0:
            out.append(f"{group}: {left} records on {topic}/{tp.partition}")
    return out


class Progress:
    """Whether the tracker (and, on ais.enriched, the Flink job before it) is done with
    the run. The run's last ais.enriched offsets are looked up once, after the Flink
    job has finished the run: no record of it can be added after that."""

    def __init__(self, brokers: str, input_topic: str, run_id: str, raw_last: dict[int, int], since_ms: int):
        self.brokers, self.input_topic, self.run_id = brokers, input_topic, run_id
        self.raw_last, self.since_ms = raw_last, since_ms
        self.enriched_last: dict[int, int] | None = None

    def tracker_behind(self) -> list[str]:
        if self.input_topic == RAW_TOPIC:
            return lagging(self.brokers, TRACKER_GROUP, RAW_TOPIC, self.raw_last)
        if self.enriched_last is None:
            flink = lagging(self.brokers, FLINK_GROUP, RAW_TOPIC, self.raw_last)
            if flink:
                return flink
            self.enriched_last = run_last_offsets(self.brokers, ENRICHED_TOPIC, self.run_id, self.since_ms)
        return lagging(self.brokers, TRACKER_GROUP, ENRICHED_TOPIC, self.enriched_last)
