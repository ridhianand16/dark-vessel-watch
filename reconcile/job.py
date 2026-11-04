"""Reconcile one replay run end to end, per ais.raw partition, and store the verdict.

    uv run python -m reconcile.job <run_id> [--timeout 600]   # or: make reconcile-run RUN=<run_id>

1. Run state. data/runs/<run_id>/produced.json must say "complete". While it says
   "running" (the replayer is still producing) the job waits; any other status is
   refused.
2. Pipeline done. Waits until every consumer group on the run's path has committed
   past the run's last offsets, read from Kafka, never guessed from time:
       ais.raw       dvw-raw-sink, dvw-flink-enrichment (commits after each checkpoint)
       ais.enriched  dvw-tracker (or ais.raw, with TRACKER_INPUT_TOPIC=ais.raw)
       ais.tracks    dvw-tracks-sink, dvw-vessels-sink
       ais.alerts    dvw-alerts-sink
   The last offsets on ais.enriched, ais.tracks and ais.alerts are the run's last
   records there, looked up once the stage before has finished the run.
3. Checks, per partition over the run's offset range [first_offset, last_offset]:
       raw_stored     produced = rows in dvw.ais_raw FINAL, none from another run
       flink_balance  raw = enriched + dlq + dedup_counts, no raw offset written twice
       zone_tagged    no enriched record without zone tags
       rollup         sum of dvw.ais_1min n_fixes = rows in dvw.tracks FINAL
       dlq_share      dlq / produced at most --dlq-warn (warn, never fail)
   and run-wide (partition -1):
       latency        p50/p95/p99 of replay send to fix stored in dvw.tracks (warn above
                      --latency-warn-ms)
   With --baseline <run_id>, also tracks_match per partition: this run's tracks equal
   the baseline's, vessel by vessel (reconcile/compare_tracks.py).
4. Prints the tables, writes one row per partition and check to dvw.reconciliation
   (a rerun replaces them), exits non-zero on any fail.

The checks reuse run_check, flink_check, latency and compare_tracks. See docs/adr/0021.

Exit codes: 0 no check failed (warnings allowed); 1 a check failed, or the pipeline did
not finish the run within --timeout (nothing is written then); 2 the run cannot be
checked.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_connect.driver.client import Client

from common.clickhouse import client_from_env
from common.env import REPO_ROOT, setting
from reconcile import compare_tracks, flink_check, kafka, latency, run_check
from reconcile.run_check import Produced, RunError, RunStillRunning
from reconcile.kafka import ALERTS_SINK_GROUP, FLINK_GROUP, Progress, lagging, run_last_offsets
from stream.faust_app.config import ENRICHED_TOPIC, RAW_TOPIC

TABLE = "dvw.reconciliation"
RAW_SINK_GROUP = "dvw-raw-sink"  # sinks/clickhouse/specs.py
TRACKS_SINK_GROUP = "dvw-tracks-sink"  # sinks/clickhouse/specs.py, runs in the alerts sink process
VESSELS_SINK_GROUP = "dvw-vessels-sink"  # sinks/postgis_vessels/sink.py
TRACKS_TOPIC = "ais.tracks"
ALERTS_TOPIC = "ais.alerts"
RUN_WIDE = -1
CHECKS = ("raw_stored", "flink_balance", "zone_tagged", "rollup", "dlq_share", "tracks_match")


@dataclass
class Counts:
    """One ais.raw partition's numbers over the run's offset range."""

    produced: int = 0
    stored: int = 0
    enriched: int = 0
    dlq: int = 0
    duplicates: int = 0
    untagged: int = 0
    tracks: int = 0
    rollup_fixes: int = 0
    dlq_reasons: Counter[str] = field(default_factory=Counter)

    @property
    def dlq_share(self) -> float:
        return self.dlq / self.produced if self.produced else 0.0

    def add(self, other: Counts) -> None:
        for name in ("produced", "stored", "enriched", "dlq", "duplicates", "untagged", "tracks", "rollup_fixes"):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.dlq_reasons.update(other.dlq_reasons)


@dataclass(frozen=True)
class Latency:
    n: int
    p50: float | None
    p95: float | None
    p99: float | None


@dataclass(frozen=True)
class Check:
    partition: int
    check: str
    status: str  # ok, warn, fail
    note: str
    expected: int
    actual: int


@dataclass(frozen=True)
class Limits:
    dlq_warn: float = 0.05
    latency_warn_ms: float = 5000.0


# ---------------------------------------------------------------- run state


def wait_for_run(path: Path, run_id: str, deadline: float, poll_s: float = 2.0) -> list[Produced]:
    """The run's produced offsets once produced.json says complete. Waits while it says
    running; a missing file or any other status is a RunError at once."""
    announced = False
    while True:
        try:
            return run_check.load_produced(path, run_id)
        except RunStillRunning:
            if time.monotonic() >= deadline:
                raise RunError(f"run {run_id} is still running (or its replayer was killed)") from None
            if not announced:
                print(f"reconcile: run {run_id} is still replaying, waiting for it to complete", file=sys.stderr)
                announced = True
            time.sleep(poll_s)


# ---------------------------------------------------------- pipeline done


class Pipeline:
    """What the consumer groups on the run's path still have to do for the run."""

    def __init__(self, brokers: str, run_id: str, produced: list[Produced], since_ms: int, tracker_input: str):
        self.brokers, self.run_id, self.since_ms = brokers, run_id, since_ms
        self.raw_last = {p.partition: p.last_offset for p in produced}
        self.tracker = Progress(brokers, tracker_input, run_id, self.raw_last, since_ms)
        self.downstream: dict[str, dict[int, int]] | None = None

    def behind(self) -> list[str]:
        out = lagging(self.brokers, RAW_SINK_GROUP, RAW_TOPIC, self.raw_last)
        # Always Flink, also when the tracker reads ais.raw: the balance check reads its output.
        out += lagging(self.brokers, FLINK_GROUP, RAW_TOPIC, self.raw_last)
        if out:
            return out
        out = self.tracker.tracker_behind()
        if out:
            return out
        if self.downstream is None:
            # The tracker sends a record's outputs before committing it (ADR 0004), so
            # every track and alert of the run is on its topic by now.
            self.downstream = {
                t: run_last_offsets(self.brokers, t, self.run_id, self.since_ms) for t in (TRACKS_TOPIC, ALERTS_TOPIC)
            }
        for group, topic in (
            (TRACKS_SINK_GROUP, TRACKS_TOPIC),
            (VESSELS_SINK_GROUP, TRACKS_TOPIC),
            (ALERTS_SINK_GROUP, ALERTS_TOPIC),
        ):
            if self.downstream[topic]:
                out += lagging(self.brokers, group, topic, self.downstream[topic])
        return out


def wait_for_pipeline(pipeline: Pipeline, deadline: float, poll_s: float = 2.0, report_s: float = 15.0) -> list[str]:
    """Empty when every group is past the run, else what was still pending at the deadline."""
    last_report = time.monotonic()
    while (pending := pipeline.behind()) and time.monotonic() < deadline:
        if time.monotonic() - last_report >= report_s:
            print(f"reconcile: waiting: {'; '.join(pending)}", file=sys.stderr)
            last_report = time.monotonic()
        time.sleep(poll_s)
    return pending


# ----------------------------------------------------------------- counts


def tracks_and_rollups(client: Client, run_id: str) -> tuple[dict[int, int], dict[int, int], int]:
    """(tracks FINAL per partition, rollup n_fixes per partition, rollup fixes on vessels
    with no track row). A vessel's partition comes from its tracks: all of one MMSI's
    records share an ais.raw partition (key = MMSI, ADR 0001)."""
    params = {"run": run_id}
    rows = client.query(
        "SELECT src_partition, mmsi, count() FROM dvw.tracks FINAL "
        "WHERE run_id = {run:String} AND decode_error = '' GROUP BY src_partition, mmsi",
        parameters=params,
    ).result_rows
    tracks: Counter[int] = Counter()
    vessel_partition: dict[int, int] = {}
    for p, mmsi, n in rows:
        tracks[int(p)] += int(n)
        vessel_partition[int(mmsi)] = int(p)
    rollup: Counter[int] = Counter()
    orphan = 0
    for mmsi, n in client.query(
        "SELECT mmsi, sum(n_fixes) FROM dvw.ais_1min_v WHERE run_id = {run:String} GROUP BY mmsi",
        parameters=params,
    ).result_rows:
        p = vessel_partition.get(int(mmsi))
        if p is None:
            orphan += int(n)
        else:
            rollup[p] += int(n)
    return dict(tracks), dict(rollup), orphan


def gather(
    produced: list[Produced],
    stored: list[run_check.Result],
    tallies: list[flink_check.Tally],
    tracks: dict[int, int],
    rollup: dict[int, int],
) -> dict[int, Counts]:
    counts = {p.partition: Counts(produced=p.count) for p in produced}
    for r in stored:
        counts[r.produced.partition].stored = r.stored.rows
    for t in tallies:
        c = counts[t.produced.partition]
        c.enriched, c.dlq, c.duplicates, c.untagged = t.enriched, t.dlq, t.duplicates, t.untagged
        c.dlq_reasons = Counter(t.reasons)
    for p, c in counts.items():
        c.tracks, c.rollup_fixes = tracks.get(p, 0), rollup.get(p, 0)
    return counts


# ----------------------------------------------------------------- checks


def evaluate(
    stored: list[run_check.Result],
    tallies: list[flink_check.Tally],
    counts: dict[int, Counts],
    orphan_rollup: int,
    limits: Limits,
) -> list[Check]:
    out: list[Check] = []
    for r in stored:
        p = r.produced.partition
        out.append(Check(p, "raw_stored", "ok" if r.ok else "fail", r.note, r.produced.count, r.stored.rows))
    for t in tallies:
        p, notes = t.produced.partition, []
        diff = t.produced.count - t.accounted
        if diff:
            notes.append(f"missing {diff}" if diff > 0 else f"extra {-diff}")
        if t.repeated:
            notes.append(f"{t.repeated} offsets twice")
        if t.foreign:
            notes.append(f"{t.foreign} from another run")
        ok = not notes
        out.append(Check(p, "flink_balance", "ok" if ok else "fail", ", ".join(notes) or "ok",
                         t.produced.count, t.accounted))
        out.append(Check(p, "zone_tagged", "fail" if t.untagged else "ok",
                         f"{t.untagged} without zone tags" if t.untagged else "ok",
                         t.enriched, t.enriched - t.untagged))
    first = min(counts) if counts else 0
    for p, c in sorted(counts.items()):
        notes = []
        if c.rollup_fixes != c.tracks:
            notes.append(f"rollup {c.rollup_fixes} vs tracks {c.tracks}")
        if p == first and orphan_rollup:  # reported once, not per partition
            notes.append(f"{orphan_rollup} rollup fixes on vessels with no track row")
        out.append(Check(p, "rollup", "fail" if notes else "ok", ", ".join(notes) or "ok", c.tracks, c.rollup_fixes))
        over = c.dlq_share > limits.dlq_warn
        reasons = ", ".join(f"{r} {n}" for r, n in c.dlq_reasons.most_common()) or "no dlq"
        note = f"{c.dlq_share:.1%} in dlq ({reasons})" + (f", above {limits.dlq_warn:.0%}" if over else "")
        out.append(Check(p, "dlq_share", "warn" if over else "ok", note, c.produced, c.dlq))
    return out


def latency_check(values: list[float], limits: Limits) -> tuple[Check, Latency]:
    if not values:
        return Check(RUN_WIDE, "latency", "warn", "no stored tracks to measure", 0, 0), Latency(0, None, None, None)
    p50, p95, p99, top = latency.quantiles(values)
    over = p99 > limits.latency_warn_ms
    note = (f"replay send to track stored: p50 {p50:.0f} ms, p95 {p95:.0f} ms, p99 {p99:.0f} ms, max {top:.0f} ms"
            + (f"; p99 above {limits.latency_warn_ms:.0f} ms" if over else ""))
    return Check(RUN_WIDE, "latency", "warn" if over else "ok", note, len(values), len(values)), Latency(
        len(values), p50, p95, p99
    )


def tracks_match(run_id: str, baseline: str, tracks: Iterable[dict[str, Any]]) -> list[Check]:
    """Per partition: vessels whose tracks equal the baseline run's, as compare_tracks
    defines it (identical, or differing only by a same-second repeat Flink dropped)."""
    msgs = list(tracks)
    partition_of = {
        int(t["mmsi"]): int(t["src_partition"]) for t in msgs if t.get("run_id") in (run_id, baseline)
    }
    a, b = compare_tracks.by_vessel(baseline, msgs), compare_tracks.by_vessel(run_id, msgs)
    vessels: Counter[int] = Counter()
    matching: Counter[int] = Counter()
    differing: dict[int, list[str]] = {}
    for m in sorted(a.keys() | b.keys()):
        p = partition_of[m]
        vessels[p] += 1
        x, y = a.get(m, []), b.get(m, [])
        if x == y or compare_tracks.only_flink_dedup(x, y):
            matching[p] += 1
        else:
            differing.setdefault(p, []).append(f"{m}: {compare_tracks.first_difference(x, y)}")
    out = []
    for p in sorted(vessels):
        bad = differing.get(p, [])
        note = f"vs {baseline}: " + (f"{len(bad)} vessels differ, first {bad[0]}" if bad else "every vessel matches")
        out.append(Check(p, "tracks_match", "fail" if bad else "ok", note, vessels[p], matching[p]))
    return out


# ----------------------------------------------------------------- output


def render(run_id: str, counts: dict[int, Counts], checks: list[Check]) -> str:
    head = (f"{'partition':>9} {'produced':>9} {'stored':>9} {'enriched':>9} {'dlq':>6} {'dups':>7} "
            f"{'untagged':>8} {'tracks':>8} {'rollup':>8}")
    lines = [f"run {run_id}", head, "-" * len(head)]
    total = Counts()
    for c in counts.values():
        total.add(c)
    for label, c in [*((str(p), c) for p, c in sorted(counts.items())), ("total", total)]:
        if label == "total":
            lines.append("-" * len(head))
        lines.append(f"{label:>9} {c.produced:>9} {c.stored:>9} {c.enriched:>9} {c.dlq:>6} {c.duplicates:>7} "
                     f"{c.untagged:>8} {c.tracks:>8} {c.rollup_fixes:>8}")
    if total.dlq_reasons:
        lines.append("dlq reasons: " + ", ".join(f"{r} {n}" for r, n in total.dlq_reasons.most_common()))
    lines.append("")
    head = f"{'check':<14} {'partition':>9} {'expected':>9} {'actual':>9}  {'status':<6} note"
    lines += [head, "-" * len(head)]
    order = {name: i for i, name in enumerate((*CHECKS, "latency"))}
    for ch in sorted(checks, key=lambda ch: (order.get(ch.check, 99), ch.partition)):
        part = "run" if ch.partition == RUN_WIDE else str(ch.partition)
        note = "" if ch.status == "ok" and ch.note == "ok" else ch.note
        lines.append(f"{ch.check:<14} {part:>9} {ch.expected:>9} {ch.actual:>9}  {ch.status.upper():<6} {note}")
    return "\n".join(lines)


COLUMNS = (
    "run_id", "partition", "check", "status", "note", "expected", "actual",
    "produced", "stored", "enriched", "dlq", "duplicates", "untagged", "tracks", "rollup_fixes",
    "dlq_reasons", "dlq_share", "latency_n", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "checked_at",
)


def rows(run_id: str, counts: dict[int, Counts], checks: list[Check], lat: Latency, now: datetime) -> list[list[Any]]:
    total = Counts()
    for c in counts.values():
        total.add(c)
    out = []
    for ch in checks:
        c = total if ch.partition == RUN_WIDE else counts.get(ch.partition, Counts())
        run_wide = ch.partition == RUN_WIDE
        out.append([
            run_id, ch.partition, ch.check, ch.status, ch.note, ch.expected, ch.actual,
            c.produced, c.stored, c.enriched, c.dlq, c.duplicates, c.untagged, c.tracks, c.rollup_fixes,
            dict(c.dlq_reasons), c.dlq_share,
            lat.n if run_wide else 0,
            lat.p50 if run_wide else None, lat.p95 if run_wide else None, lat.p99 if run_wide else None,
            now,
        ])
    return out


# ------------------------------------------------------------------- main


def check_once(
    client: Client, brokers: str, run_id: str, produced: list[Produced], since_ms: int,
) -> tuple[list[run_check.Result], list[flink_check.Tally], dict[int, int], dict[int, int], int]:
    stored = run_check.compare(produced, run_check.query_stored(client, run_id, produced))
    tallies, _ = flink_check.tally(
        run_id, produced,
        kafka.read_topic(brokers, flink_check.ENRICHED, since_ms),
        kafka.read_topic(brokers, flink_check.DLQ, since_ms),
        kafka.read_topic(brokers, flink_check.COUNTS, since_ms),
    )
    tracks, rollup, orphan = tracks_and_rollups(client, run_id)
    return stored, tallies, tracks, rollup, orphan


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="reconcile", description=__doc__.splitlines()[0])
    ap.add_argument("run_id")
    ap.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "data" / "runs")
    ap.add_argument("--timeout", type=float, default=600.0,
                    help="seconds to wait for the replay to complete and the pipeline to finish the run")
    ap.add_argument("--settle", type=float, default=30.0,
                    help="seconds to re-check while a count is only short (a last Flink transaction committing)")
    ap.add_argument("--dlq-warn", type=float, default=0.05, help="warn when dlq / produced exceeds this share")
    ap.add_argument("--latency-warn-ms", type=float, default=5000.0, help="warn when p99 latency exceeds this")
    ap.add_argument("--baseline", help="an earlier run of the same rows: also compare tracks vessel by vessel")
    ap.add_argument("--no-write", action="store_true", help=f"print only, do not write to {TABLE}")
    args = ap.parse_args(argv)
    limits = Limits(args.dlq_warn, args.latency_warn_ms)

    run_id = args.run_id
    path = args.runs_dir / run_id / "produced.json"
    brokers = setting("REDPANDA_BROKERS", "localhost:19092")
    deadline = time.monotonic() + args.timeout
    try:
        produced = wait_for_run(path, run_id, deadline)
        since_ms = kafka.run_started_ms(path) - kafka.SEEK_MARGIN_MS
        baseline_since = None
        if args.baseline:
            baseline_since = kafka.run_started_ms(args.runs_dir / args.baseline / "produced.json")
    except (RunError, FileNotFoundError) as e:
        print(f"reconcile: {e}", file=sys.stderr)
        return 2

    tracker_input = setting("TRACKER_INPUT_TOPIC", ENRICHED_TOPIC)
    try:
        pending = wait_for_pipeline(Pipeline(brokers, run_id, produced, since_ms, tracker_input), deadline)
    except RunError as e:  # a topic read that timed out
        print(f"reconcile: {e}", file=sys.stderr)
        return 2
    if pending:
        print(f"reconcile: INCOMPLETE, the pipeline did not finish run {run_id} within {args.timeout:.0f} s: "
              f"{'; '.join(pending)}. Nothing written.", file=sys.stderr)
        return 1

    client = client_from_env()
    try:
        settle = time.monotonic() + args.settle
        while True:
            stored, tallies, tracks, rollup, orphan = check_once(client, brokers, run_id, produced, since_ms)
            counts = gather(produced, stored, tallies, tracks, rollup)
            checks = evaluate(stored, tallies, counts, orphan, limits)
            # Flink commits its source offsets and its sink transactions in separate
            # callbacks after a checkpoint, so the last transaction can become visible
            # a moment after the offsets. Re-check briefly while counts are only short.
            short = all(n.produced >= n.stored and n.produced >= n.enriched + n.dlq + n.duplicates
                        for n in counts.values())
            failed = any(c.status == "fail" for c in checks)
            if not failed or not short or time.monotonic() >= settle:
                break
            time.sleep(2.0)
        lat_check, lat = latency_check(latency.stored_track_latencies(client, run_id), limits)
        checks.append(lat_check)
        if args.baseline and baseline_since is not None:
            since = min(since_ms, baseline_since - kafka.SEEK_MARGIN_MS)
            checks += tracks_match(run_id, args.baseline, kafka.read_topic(brokers, TRACKS_TOPIC, since))
    except RunError as e:
        print(f"reconcile: {e}", file=sys.stderr)
        return 2

    print(render(run_id, counts, checks))
    if not args.no_write:
        client.insert(TABLE, rows(run_id, counts, checks, lat, datetime.now(UTC)), column_names=list(COLUMNS))
        print(f"reconcile: wrote {len(checks)} rows to {TABLE}")
    fails = [c for c in checks if c.status == "fail"]
    warns = [c for c in checks if c.status == "warn"]
    if fails:
        print(f"reconcile: FAIL, {len(fails)} checks failed: "
              + ", ".join(sorted({c.check for c in fails})), file=sys.stderr)
        return 1
    total = sum(p.count for p in produced)
    print(f"reconcile: OK, {total} produced, every check balances on all {len(produced)} partitions"
          + (f" ({len(warns)} warning{'s' if len(warns) > 1 else ''})" if warns else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
