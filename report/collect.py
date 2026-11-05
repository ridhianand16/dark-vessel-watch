"""Numbers read from ClickHouse once the pipeline has finished the run.

The reconciliation verdict is read back from dvw.reconciliation, where `make
reconcile-run` stored it, rather than recomputed: the report shows what the job
decided. Detection is scored with reconcile.score_alerts, latency with
reconcile.latency.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from clickhouse_connect.driver.client import Client

from reconcile import latency, score_alerts
from reconcile.run_check import RunError

Quantiles = tuple[int, float, float, float, float]  # n, p50, p95, p99, max (ms)


def quantiles(values: list[float]) -> Quantiles | None:
    if not values:
        return None
    p50, p95, p99, top = latency.quantiles(values)
    return len(values), p50, p95, p99, top


@dataclass(frozen=True)
class Latencies:
    track_stored: Quantiles | None
    track_stored_before_faults: Quantiles | None  # None without faults
    alert_stored: Quantiles | None


def latencies(client: Client, run_id: str, first_fault_ms: int | None) -> Latencies:
    before = None
    if first_fault_ms is not None:
        before = quantiles(latency.stored_track_latencies(client, run_id, sent_before_ms=first_fault_ms))
    return Latencies(
        quantiles(latency.stored_track_latencies(client, run_id)),
        before,
        quantiles(latency.alert_latencies(client, run_id)),
    )


@dataclass(frozen=True)
class CheckRow:
    partition: int
    check: str
    status: str
    note: str
    expected: int
    actual: int


@dataclass(frozen=True)
class Totals:
    produced: int
    stored: int
    enriched: int
    dlq: int
    duplicates: int
    untagged: int
    tracks: int
    rollup_fixes: int


@dataclass(frozen=True)
class Reconciliation:
    checks: tuple[CheckRow, ...]
    totals: Totals
    dlq_reasons: dict[str, int]

    @property
    def failed(self) -> list[CheckRow]:
        return [c for c in self.checks if c.status == "fail"]


def reconciliation(client: Client, run_id: str) -> Reconciliation | None:
    """The run's rows in dvw.reconciliation FINAL; None when the job wrote none."""
    rows = client.query(
        "SELECT partition, check, toString(status), note, expected, actual, produced, stored, enriched, dlq, "
        "duplicates, untagged, tracks, rollup_fixes, dlq_reasons "
        "FROM dvw.reconciliation FINAL WHERE run_id = {run:String} ORDER BY check, partition",
        parameters={"run": run_id},
    ).result_rows
    if not rows:
        return None
    checks = tuple(CheckRow(int(r[0]), str(r[1]), str(r[2]), str(r[3]), int(r[4]), int(r[5])) for r in rows)
    # Partition counts are repeated on every check row of the partition; take them once,
    # from the row every partition has.
    per_partition = [r for r in rows if r[1] == "raw_stored"]
    sums = [sum(int(r[i]) for r in per_partition) for i in range(6, 14)]
    reasons: Counter[str] = Counter()
    for r in per_partition:
        reasons.update({str(k): int(v) for k, v in dict(r[14]).items()})
    return Reconciliation(checks, Totals(*sums), dict(reasons.most_common()))


@dataclass(frozen=True)
class Copies:
    """One table's rows for the run: as inserted, after FINAL, and distinct identities."""

    table: str
    identity: str
    inserted: int  # without FINAL: includes copies a re-insert added, until merged
    final: int
    distinct: int

    @property
    def duplicates(self) -> int:
        return self.final - self.distinct


DUPLICATE_CHECKS = (
    ("dvw.ais_raw", "kafka_partition, kafka_offset", "1"),
    ("dvw.tracks", "src_partition, src_offset", "decode_error = ''"),
    ("dvw.alerts", "alert_id", "decode_error = ''"),
)


def copies(client: Client, run_id: str) -> list[Copies]:
    out = []
    for table, identity, where in DUPLICATE_CHECKS:
        cond = f"run_id = {{run:String}} AND {where}"
        inserted = client.query(f"SELECT count() FROM {table} WHERE {cond}", parameters={"run": run_id}).result_rows
        final, distinct = client.query(
            f"SELECT count(), uniqExact({identity}) "
            f"FROM {table} FINAL WHERE {cond}",
            parameters={"run": run_id},
        ).result_rows[0]
        out.append(Copies(table, identity, int(inserted[0][0]), int(final), int(distinct)))
    return out


@dataclass(frozen=True)
class Detection:
    events: int
    decoys: int
    rows_injected: int
    tp: dict[str, int]
    fp: dict[str, int]
    fn: dict[str, int]
    real: dict[str, int]  # alerts on real vessels, not scored
    zones: tuple[str, ...]
    missed: tuple[str, ...]
    unexpected: tuple[str, ...]
    alert_rows: int  # dvw.alerts FINAL rows for the run
    alert_ids: int  # distinct alert_id among them


def detection(client: Client, run_dir: Path, run_id: str) -> Detection | None:
    """Scores per alert type, or None when the run was not replayed with --inject."""
    try:
        doc = score_alerts.load_labels(run_dir / "labels.json", run_id)
    except RunError:
        return None
    stored, n, distinct = score_alerts.query(client, run_id)
    s = score_alerts.score(doc, stored)
    return Detection(
        events=len(doc["labels"]),
        decoys=sum(1 for lb in doc["labels"] if lb["expect"] is None),
        rows_injected=int(doc["rows_injected"]),
        tp={t: s.tp[t] for t in score_alerts.TYPES},
        fp={t: s.fp[t] for t in score_alerts.TYPES},
        fn={t: s.fn[t] for t in score_alerts.TYPES},
        real={t: s.real[t] for t in score_alerts.TYPES},
        zones=tuple(sorted(s.zones)),
        missed=tuple(s.missed),
        unexpected=tuple(s.unexpected),
        alert_rows=n,
        alert_ids=distinct,
    )
