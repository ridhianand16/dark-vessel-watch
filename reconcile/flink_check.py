"""Check that the Flink enrichment job accounted for every message of one replay run.

Used by the reconciliation job (reconcile/job.py, make reconcile-run RUN=<run_id>) as its
flink_balance and zone_tagged checks. The topics are read with reconcile/kafka.py.

For each ais.raw partition p in data/runs/<run_id>/produced.json, over its offset range
[first_offset, last_offset]:

    produced(p) = enriched(p) + dlq(p) + duplicates(p)

enriched(p) and dlq(p) count the ais.enriched and ais.dlq records whose src_partition is p and
whose src_offset is in the range. duplicates(p) sums `count` over the ais.dedup_counts records
with this run_id and src_partition p (docs/adr/0015). It also checks that no
(src_partition, src_offset) appears twice across ais.enriched and ais.dlq, that every
record in the range carries this run_id (a decode_error DLQ record may have none), and
that every enriched record is zone-tagged: zones_version set, which the job writes on every
record it tagged (docs/adr/0018). Null distances are a valid tag, for a point the job
could not project. The job buffers records until the zones are loaded, so an untagged
record means that buffer leaked.

All three topics are read with isolation.level=read_committed, so only output of completed
Flink checkpoints counts, starting at the run's start time (offsets_for_times) minus a minute.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from reconcile.run_check import Produced

ENRICHED = "ais.enriched"
DLQ = "ais.dlq"
COUNTS = "ais.dedup_counts"


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
            if rec.get("zones_version") is None:
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
