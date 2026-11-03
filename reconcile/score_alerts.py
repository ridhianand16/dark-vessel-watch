"""Score the alerts of one replay run against its injected labels.

    uv run python -m reconcile.score_alerts <run_id> [--wait 60]   # or: make score RUN=<run_id>

Reads data/runs/<run_id>/labels.json (written by the replayer's --inject) and the run's
rows in dvw.alerts FINAL. Matching is exact: a label expects the alert_id the tracker
derives from (type, mmsi, run_id, trigger_ts) (ADR 0005), so an alert raised at the
wrong time counts as one FP and one FN, not as a hit.

Per alert type:
    TP  labels whose expected alert is stored
    FN  labels whose expected alert is not stored
    FP  any other alert on a synthetic MMSI: a second alert for one event, the wrong
        type, or any alert on a decoy (labels with expect: null)
Alerts on real vessels have no ground truth; they are counted separately, not scored.

Scores only once the pipeline has finished the run: the tracker has committed past the
run's last ais.raw offsets (produced.json) and the alerts sink has committed up to the
end of ais.alerts. Stopping as soon as the expected alerts appear would miss false
positives raised later in the run. --wait bounds how long to wait for that.

Also checks that FINAL holds each alert_id once. Exit codes: 0 no FP, no FN and no
duplicate; 1 otherwise, or the pipeline did not finish within --wait; 2 the run
cannot be scored.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer, TopicPartition

from common.clickhouse import client_from_env
from common.env import REPO_ROOT, setting
from reconcile.run_check import RunError, load_produced
from stream.faust_app.detect import AlertType, alert_id

TABLE = "dvw.alerts"
TYPES = tuple(t.value for t in AlertType)
TRACKER_GROUP = "dvw-tracker"
ALERTS_SINK_GROUP = "dvw-alerts-sink"


@dataclass(frozen=True)
class Stored:
    alert_id: str
    type: str
    mmsi: int | None


@dataclass
class Score:
    tp: Counter[str] = field(default_factory=Counter)
    fp: Counter[str] = field(default_factory=Counter)
    fn: Counter[str] = field(default_factory=Counter)
    real: Counter[str] = field(default_factory=Counter)
    missed: list[str] = field(default_factory=list)
    unexpected: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.fp and not self.fn


def load_labels(path: Path, run_id: str) -> dict[str, Any]:
    try:
        doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise RunError(f"{path} not found; was the run replayed with --inject?") from None
    if doc.get("run_id") != run_id:
        raise RunError(f"{path} is for run {doc.get('run_id')!r}, not {run_id!r}")
    if doc.get("status") != "complete":
        raise RunError(f"run {run_id} has status {doc.get('status')!r}; only complete runs can be scored")
    if doc.get("csv_rows_in_mmsi_range"):
        raise RunError(f"{doc['csv_rows_in_mmsi_range']} real rows use the synthetic MMSI range; labels are ambiguous")
    return doc


def expected_ids(doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """alert_id -> label, for labels that expect an alert."""
    run_id = doc["run_id"]
    return {
        alert_id(AlertType(lb["expect"]), lb["mmsi"], run_id, lb["trigger_ts"]): lb
        for lb in doc["labels"]
        if lb["expect"] is not None
    }


def score(doc: dict[str, Any], stored: list[Stored]) -> Score:
    lo, hi = doc["mmsi_range"]
    expected = expected_ids(doc)
    names = {lb["mmsi"]: lb["name"] for lb in doc["labels"]}
    s = Score()
    found = set()
    for a in stored:
        if a.mmsi is None or not lo <= a.mmsi <= hi:
            s.real[a.type] += 1
        elif a.alert_id in expected:
            s.tp[a.type] += 1
            found.add(a.alert_id)
        else:
            s.fp[a.type] += 1
            s.unexpected.append(f"{a.type} on {names.get(a.mmsi, a.mmsi)} ({a.alert_id})")
    for aid, lb in expected.items():
        if aid not in found:
            s.fn[lb["expect"]] += 1
            s.missed.append(f"{lb['expect']} on {lb['name']} at trigger_ts {lb['trigger_ts']} ({aid})")
    return s


def behind(brokers: str, raw_last: dict[int, int]) -> list[str]:
    """What the pipeline still has to process for the run; empty when it is done.

    raw_last is the run's last ais.raw offset per partition. Committed offsets are the
    next offset to read, so the tracker is done with a partition once its commit is past
    the last one. Reading a group's commits does not join the group.
    """
    out: list[str] = []

    def committed(group: str, tps: list[TopicPartition]) -> list[TopicPartition]:
        c = Consumer({"bootstrap.servers": brokers, "group.id": group, "enable.auto.commit": False})
        try:
            return list(c.committed(tps, timeout=10))
        finally:
            c.close()

    raw = committed(TRACKER_GROUP, [TopicPartition("ais.raw", p) for p in sorted(raw_last)])
    for tp in raw:
        left = raw_last[tp.partition] + 1 - max(tp.offset, 0)
        if left > 0:
            out.append(f"tracker: {left} records on ais.raw/{tp.partition}")
    c = Consumer({"bootstrap.servers": brokers, "group.id": ALERTS_SINK_GROUP})
    try:
        meta = c.list_topics("ais.alerts", timeout=10).topics["ais.alerts"].partitions
        ends = {p: c.get_watermark_offsets(TopicPartition("ais.alerts", p), timeout=10)[1] for p in meta}
    finally:
        c.close()
    for tp in committed(ALERTS_SINK_GROUP, [TopicPartition("ais.alerts", p) for p in sorted(ends)]):
        left = ends[tp.partition] - max(tp.offset, 0)
        if left > 0:
            out.append(f"alerts-sink: {left} records on ais.alerts/{tp.partition}")
    return out


def query(client: Client, run_id: str) -> tuple[list[Stored], int, int]:
    """(rows FINAL, count FINAL, distinct alert_id FINAL) for the run."""
    params = {"run": run_id}
    rows = client.query(
        f"SELECT alert_id, type, mmsi FROM {TABLE} FINAL WHERE run_id = {{run:String}}", parameters=params
    ).result_rows
    n, distinct = client.query(
        f"SELECT count(), uniqExact(alert_id) FROM {TABLE} FINAL WHERE run_id = {{run:String}}", parameters=params
    ).result_rows[0]
    return [Stored(str(a), str(t), None if m is None else int(m)) for a, t, m in rows], int(n), int(distinct)


def render(run_id: str, doc: dict[str, Any], s: Score, n: int, distinct: int) -> str:
    decoys = sum(1 for lb in doc["labels"] if lb["expect"] is None)
    lines = [
        f"run {run_id}: {len(doc['labels'])} injected events ({decoys} decoys), "
        f"{doc['rows_injected']} synthetic rows",
        f"{'type':<10} {'TP':>4} {'FP':>4} {'FN':>4} {'precision':>10} {'recall':>8} {'real-traffic':>13}",
    ]
    for t in TYPES:
        tp, fp, fn = s.tp[t], s.fp[t], s.fn[t]
        prec = f"{tp / (tp + fp):.2f}" if tp + fp else "-"
        rec = f"{tp / (tp + fn):.2f}" if tp + fn else "-"
        lines.append(f"{t:<10} {tp:>4} {fp:>4} {fn:>4} {prec:>10} {rec:>8} {s.real[t]:>13}")
    lines.append(f"{TABLE} FINAL for this run: {n} rows, {distinct} distinct alert_id")
    lines += [f"  missed: {m}" for m in s.missed]
    lines += [f"  unexpected: {u}" for u in s.unexpected]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="score_alerts", description=__doc__.splitlines()[0])
    ap.add_argument("run_id")
    ap.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "data" / "runs")
    ap.add_argument("--wait", type=float, default=0.0,
                    help="seconds to wait for the tracker and alerts sink to finish the run")
    args = ap.parse_args(argv)
    run_dir = args.runs_dir / args.run_id
    try:
        doc = load_labels(run_dir / "labels.json", args.run_id)
        raw_last = {p.partition: p.last_offset for p in load_produced(run_dir / "produced.json", args.run_id)}
    except RunError as e:
        print(f"score: {e}", file=sys.stderr)
        return 2
    brokers = setting("REDPANDA_BROKERS", "localhost:19092")
    deadline = time.monotonic() + args.wait
    while (pending := behind(brokers, raw_last)) and time.monotonic() < deadline:
        time.sleep(2.0)
    stored, n, distinct = query(client_from_env(), args.run_id)
    s = score(doc, stored)
    print(render(args.run_id, doc, s, n, distinct))
    if pending:
        print(f"score: INCOMPLETE, the pipeline has not finished the run: {'; '.join(pending)}", file=sys.stderr)
        return 1
    if s.ok and n == distinct:
        print("score: OK, every injected event raised exactly its alert")
        return 0
    print("score: MISMATCH", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
