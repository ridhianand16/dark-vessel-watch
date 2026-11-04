"""Check that every message of one replay run is stored in dvw.ais_raw.

Used by the reconciliation job (reconcile/job.py, make reconcile-run RUN=<run_id>) as
its raw_stored check, and by the other reconcile tools to read produced.json.

Reads data/runs/<run_id>/produced.json (offsets the broker acknowledged, per partition)
and refuses unless its status is "complete". For each partition it counts rows in
dvw.ais_raw FINAL with kafka_offset in [first_offset, last_offset] and compares that
with the produced count, and checks every row in the range carries this run_id.
FINAL is required: without it, a batch the sink re-inserted after a crash is counted
twice until a background merge collapses it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from clickhouse_connect.driver.client import Client

TABLE = "dvw.ais_raw"


class RunError(Exception):
    pass


class RunStillRunning(RunError):
    """produced.json says "running": the replayer has not finished the run (or was killed)."""


@dataclass(frozen=True)
class Produced:
    partition: int
    first_offset: int
    last_offset: int
    count: int


@dataclass(frozen=True)
class Stored:
    rows: int  # count() FINAL in the offset range
    foreign: int  # of those, rows whose run_id is not this run
    unmerged: int  # count() without FINAL, for information only


@dataclass(frozen=True)
class Result:
    produced: Produced
    stored: Stored

    @property
    def ok(self) -> bool:
        return self.stored.rows == self.produced.count and self.stored.foreign == 0

    @property
    def missing(self) -> int:
        return max(self.produced.count - self.stored.rows, 0)

    @property
    def note(self) -> str:
        notes = []
        if self.stored.rows < self.produced.count:
            notes.append(f"missing {self.produced.count - self.stored.rows}")
        elif self.stored.rows > self.produced.count:
            notes.append(f"extra {self.stored.rows - self.produced.count}")
        if self.stored.foreign:
            notes.append(f"{self.stored.foreign} rows from another run")
        return ", ".join(notes) or "ok"


def load_produced(path: Path, run_id: str) -> list[Produced]:
    try:
        doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise RunError(f"{path} not found") from None
    if doc.get("run_id") != run_id:
        raise RunError(f"{path} is for run {doc.get('run_id')!r}, not {run_id!r}")
    if doc.get("status") == "running":
        raise RunStillRunning(f"run {run_id} has status 'running'; only complete runs can be reconciled")
    if doc.get("status") != "complete":
        raise RunError(f"run {run_id} has status {doc.get('status')!r}; only complete runs can be reconciled")
    out = []
    for p, s in sorted(doc["partitions"].items(), key=lambda kv: int(kv[0])):
        first, last, count = int(s["first_offset"]), int(s["last_offset"]), int(s["count"])
        if count <= 0 or last < first or count > last - first + 1:
            raise RunError(f"partition {p}: inconsistent range {first}..{last} for {count} messages")
        out.append(Produced(int(p), first, last, count))
    if not out:
        raise RunError(f"run {run_id} produced no messages")
    return out


def compare(produced: list[Produced], stored: dict[int, Stored]) -> list[Result]:
    return [Result(p, stored.get(p.partition, Stored(0, 0, 0))) for p in produced]


def query_stored(client: Client, run_id: str, produced: list[Produced]) -> dict[int, Stored]:
    # Offsets are ints parsed above, so formatting them into the SQL is safe.
    ranges = " OR ".join(
        f"(kafka_partition = {p.partition} AND kafka_offset BETWEEN {p.first_offset} AND {p.last_offset})"
        for p in produced
    )
    final = client.query(
        f"SELECT kafka_partition, count(), countIf(run_id != {{run:String}}) "
        f"FROM {TABLE} FINAL WHERE {ranges} GROUP BY kafka_partition",
        parameters={"run": run_id},
    ).result_rows
    plain = client.query(f"SELECT kafka_partition, count() FROM {TABLE} WHERE {ranges} GROUP BY kafka_partition")
    unmerged: dict[int, int] = {int(p): int(n) for p, n in plain.result_rows}
    return {int(p): Stored(int(n), int(f), unmerged.get(int(p), 0)) for p, n, f in final}
