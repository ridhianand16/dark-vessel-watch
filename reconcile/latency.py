"""End-to-end latency of one replay run.

    uv run python -m reconcile.latency <run_id>   # or: make latency RUN=<run_id>

Two measurements, both starting at replay_send_ts, the wall time the replayer handed
the record to its producer:

    tracks  every ais.tracks message of the run: the message's Kafka timestamp (set
            by the tracker's producer when it sent the track) minus the source
            record's replay_send_ts, which the track carries. This is the whole path
            through the Flink job and the tracker, for every accepted fix.
    alerts  TELEPORT rows of the run in dvw.alerts: ingested_at (the ClickHouse
            insert) minus replay_send_ts of the ais.raw record with the alert's
            trace_id. This adds the alerts sink, but there are only a few per run.
            DARK_GAP is left out: its trace_id is the last report before the silence,
            sent long before the gap could be detected.

The replayer runs on the host and the services in the Docker VM, so the numbers also
include any clock offset between the two (OrbStack keeps them in sync to well under
a millisecond).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from confluent_kafka import OFFSET_END, Consumer, KafkaError, KafkaException, TopicPartition

from common.clickhouse import client_from_env
from common.env import REPO_ROOT, setting
from reconcile.flink_check import SEEK_MARGIN_MS, run_started_ms
from reconcile.run_check import RunError

TRACKS = "ais.tracks"


def percentiles(values: list[float]) -> str:
    if not values:
        return "no samples"
    v = sorted(values)

    def p(q: float) -> float:
        return v[min(len(v) - 1, int(q * len(v)))]

    return f"n={len(v)} p50={p(0.50):.0f} ms p95={p(0.95):.0f} ms p99={p(0.99):.0f} ms max={v[-1]:.0f} ms"


def track_latencies(brokers: str, run_id: str, since_ms: int) -> list[float]:
    c = Consumer({
        "bootstrap.servers": brokers,
        "group.id": "dvw-latency",  # never committed
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    out: list[float] = []
    try:
        parts = c.list_topics(TRACKS, timeout=10).topics[TRACKS].partitions
        start = c.offsets_for_times([TopicPartition(TRACKS, p, since_ms) for p in parts], timeout=10)
        c.assign([TopicPartition(TRACKS, tp.partition, tp.offset if tp.offset >= 0 else OFFSET_END) for tp in start])
        remaining = set(parts)
        while remaining:
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
            if value is None:
                continue
            t = json.loads(value)
            sent = t.get("replay_send_ts")
            if t.get("run_id") == run_id and isinstance(sent, int):
                out.append(float(msg.timestamp()[1] - sent))
    finally:
        c.close()
    return out


def alert_latencies(run_id: str) -> list[float]:
    rows = client_from_env().query(
        """
        SELECT dateDiff('millisecond', r.replay_send_ts, a.ingested_at)
        FROM (SELECT trace_id, min(ingested_at) AS ingested_at FROM dvw.alerts
              WHERE run_id = {run:String} AND type = 'TELEPORT' GROUP BY trace_id) AS a
        INNER JOIN (SELECT trace_id, any(replay_send_ts) AS replay_send_ts FROM dvw.ais_raw
                    WHERE run_id = {run:String} GROUP BY trace_id) AS r USING trace_id
        """,
        parameters={"run": run_id},
    ).result_rows
    return [float(r[0]) for r in rows if r[0] is not None]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="latency", description=__doc__.splitlines()[0])
    ap.add_argument("run_id")
    ap.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "data" / "runs")
    args = ap.parse_args(argv)
    try:
        since = run_started_ms(args.runs_dir / args.run_id / "produced.json") - SEEK_MARGIN_MS
    except (RunError, FileNotFoundError) as e:
        print(f"latency: {e}", file=sys.stderr)
        return 2
    print(f"run {args.run_id}, from replay_send_ts")
    tracks = track_latencies(setting("REDPANDA_BROKERS", "localhost:19092"), args.run_id, since)
    print(f"  to track sent (ais.tracks):      {percentiles(tracks)}")
    print(f"  to TELEPORT stored (dvw.alerts): {percentiles(alert_latencies(args.run_id))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
