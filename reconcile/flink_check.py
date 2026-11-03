"""Check that the Flink enrichment job accounted for every message of one replay run.

Used by the reconciliation job (reconcile/job.py, make reconcile-run RUN=<run_id>) as its
flink_balance and zone_tagged checks; read_topic is shared by the other reconcile tools.

For each ais.raw partition p in data/runs/<run_id>/produced.json, over its offset range
[first_offset, last_offset]:

    produced(p) = enriched(p) + dlq(p) + duplicates(p)

enriched(p) and dlq(p) count the ais.enriched and ais.dlq records whose src_partition is p and
whose src_offset is in the range. duplicates(p) sums `count` over the ais.dedup_counts records
with this run_id and src_partition p (docs/adr/0015). It also checks that no
(src_partition, src_offset) appears twice across ais.enriched and ais.dlq, that every
record in the range carries this run_id (a decode_error DLQ record may have none), and
that every enriched record is zone-tagged: zones_version and dist_eez_boundary_m set
(docs/adr/0018). The job buffers records until the zones are loaded, so an untagged
record means that buffer leaked.

All three topics are read with isolation.level=read_committed, so only output of completed
Flink checkpoints counts, starting at the run's start time (offsets_for_times) minus a minute.
"""

from __future__ import annotations

import json
import time
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from confluent_kafka import OFFSET_END, Consumer, KafkaError, KafkaException, TopicPartition

from reconcile.run_check import Produced, RunError

ENRICHED = "ais.enriched"
DLQ = "ais.dlq"
COUNTS = "ais.dedup_counts"
SEEK_MARGIN_MS = 60_000


@dataclass
class Tally:
    """What the job wrote for one ais.raw partition's offset range."""

    produced: Produced
    enriched: int = 0
    dlq: int = 0
    duplicates: int = 0
    repeated: int = 0  # offsets seen a second time, in either output topic
    foreign: int = 0  # records in the range whose run_id is another run
    untagged: int = 0  # enriched records without zone tags
    reasons: Counter[str] = field(default_factory=Counter)  # DLQ reason codes
    seen: bytearray = field(default_factory=bytearray, repr=False)  # one bit per offset

    def __post_init__(self) -> None:
        if not self.seen:
            self.seen = bytearray((self.produced.last_offset - self.produced.first_offset) // 8 + 1)

    @property
    def accounted(self) -> int:
        return self.enriched + self.dlq + self.duplicates

    @property
    def ok(self) -> bool:
        return (
            self.accounted == self.produced.count and self.repeated == 0 and self.foreign == 0 and self.untagged == 0
        )

    @property
    def note(self) -> str:
        notes = []
        diff = self.produced.count - self.accounted
        if diff > 0:
            notes.append(f"missing {diff}")
        elif diff < 0:
            notes.append(f"extra {-diff}")
        if self.repeated:
            notes.append(f"{self.repeated} offsets twice")
        if self.foreign:
            notes.append(f"{self.foreign} from another run")
        if self.untagged:
            notes.append(f"{self.untagged} without zone tags")
        return ", ".join(notes) or "ok"

    def mark(self, offset: int) -> bool:
        """Record one output for this raw offset; False if it was already seen."""
        i = offset - self.produced.first_offset
        byte, bit = divmod(i, 8)
        if self.seen[byte] >> bit & 1:
            return False
        self.seen[byte] |= 1 << bit
        return True


def tally(
    run_id: str,
    produced: list[Produced],
    enriched: Iterable[dict[str, Any]],
    dlq: Iterable[dict[str, Any]],
    counts: Iterable[dict[str, Any]],
) -> tuple[list[Tally], Counter[str]]:
    """Per-partition tallies, plus DLQ reason codes for this run's range."""
    by_partition = {p.partition: Tally(p) for p in produced}
    reasons: Counter[str] = Counter()

    def place(rec: dict[str, Any]) -> Tally | None:
        t = by_partition.get(rec.get("src_partition", -1))
        off = rec.get("src_offset", -1)
        if t is None or not t.produced.first_offset <= off <= t.produced.last_offset:
            return None
        if not t.mark(off):
            t.repeated += 1
        rid = rec.get("run_id")
        if rid is not None and rid != run_id:
            t.foreign += 1
        return t

    for rec in enriched:
        if (t := place(rec)) is not None:
            t.enriched += 1
            if rec.get("zones_version") is None or rec.get("dist_eez_boundary_m") is None:
                t.untagged += 1
    for rec in dlq:
        if (t := place(rec)) is not None:
            t.dlq += 1
            t.reasons[str(rec.get("reason"))] += 1
            reasons[str(rec.get("reason"))] += 1
    for rec in counts:
        if rec.get("run_id") == run_id and (t := by_partition.get(rec.get("src_partition", -1))) is not None:
            t.duplicates += int(rec["count"])
    return list(by_partition.values()), reasons


def read_topic(brokers: str, topic: str, since_ms: int, timeout_s: float = 120.0) -> Iterator[dict[str, Any]]:
    """Every committed record of `topic` written at or after since_ms, up to the current end.

    With read_committed, the end of a partition is its last stable offset: records of a
    transaction Flink has not committed yet are neither returned nor waited for.
    """
    c = Consumer({
        "bootstrap.servers": brokers,
        "group.id": f"dvw-flink-check-{uuid.uuid4().hex[:8]}",  # never committed
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
        deadline = time.monotonic() + timeout_s
        while remaining:
            if time.monotonic() > deadline:
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
            value = msg.value()
            if value is not None:
                yield json.loads(value)
    finally:
        c.close()


def run_started_ms(path: Path) -> int:
    doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return int(datetime.fromisoformat(doc["started_at"]).timestamp() * 1000)
