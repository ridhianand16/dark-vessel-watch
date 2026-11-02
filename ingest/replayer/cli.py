"""Replay an AIS CSV into ais.raw.

    uv run python -m ingest.replayer --file data/ais/aisdk-2026-10-28.csv --speed 50
    make replay ARGS="--limit 10000 --speed 100"

Writes data/runs/<run_id>/produced.json at the end (also after Ctrl-C or delivery
failures, with status != "complete"). Exit codes: 0 every message acknowledged,
1 delivery errors or undelivered messages, 2 bad arguments/input, 130 interrupted.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from common.env import REPO_ROOT, dotenv_value
from ingest.replayer.delivery import DeliveryTracker
from ingest.replayer.pacing import MAX_SPEED, MIN_SPEED, Pacer
from ingest.replayer.record import build_message
from ingest.replayer.source import SourceError, read_layout, sorted_lines

TOPIC = "ais.raw"
FLUSH_TIMEOUT_S = 120.0


def speed_arg(s: str) -> float:
    v = float(s)
    if not MIN_SPEED <= v <= MAX_SPEED:
        raise argparse.ArgumentTypeError(f"must be between {MIN_SPEED:g} and {MAX_SPEED:g}")
    return v


def positive_int(s: str) -> int:
    v = int(s)
    if v <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return v


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="replayer", description=__doc__.splitlines()[0])
    p.add_argument("--file", type=Path, required=True, help="AIS CSV (DMA daily format)")
    p.add_argument("--speed", type=speed_arg, default=1.0, help="replay speed factor, 1 to 100")
    p.add_argument("--limit", type=positive_int, help="send only the first N rows in event-time order")
    p.add_argument(
        "--brokers",
        default=dotenv_value("REDPANDA_BROKERS") or "localhost:19092",
        help="bootstrap servers (default: REDPANDA_BROKERS from env or .env)",
    )
    p.add_argument("--topic", default=TOPIC, help=argparse.SUPPRESS)
    p.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "data" / "runs")
    return p.parse_args(argv)


def make_producer(brokers: str, run_id: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": brokers,
            "client.id": f"replayer-{run_id}",
            # Idempotence: broker dedups producer retries by (producer id, sequence),
            # so a retried batch cannot appear twice or out of order in a partition.
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "lz4",
            # Java-client-compatible hashing; see docs/adr/0001-kafka-partitioner-murmur2.md
            "partitioner": "murmur2_random",
        }
    )


def poll_until(producer: Producer, seconds: float) -> None:
    """Sleep replacement that keeps serving delivery callbacks."""
    deadline = time.monotonic() + seconds
    while (remaining := deadline - time.monotonic()) > 0:
        producer.poll(remaining)


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="milliseconds")


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    try:
        layout = read_layout(args.file)
    except (OSError, SourceError) as e:
        print(f"replayer: {e}", file=sys.stderr)
        return 2
    print(
        f"replayer: run {run_id} file={args.file} speed={args.speed:g}x limit={args.limit} "
        f"brokers={args.brokers} ts_format={layout.timestamp_format!r}",
        file=sys.stderr,
    )

    producer = make_producer(args.brokers, run_id)
    tracker = DeliveryTracker()
    pacer = Pacer(args.speed, sleep=lambda s: poll_until(producer, s))
    started_at, t0 = utc_now(), time.monotonic()
    sent = 0
    status = "complete"
    try:
        for lines, ts_batch in sorted_lines(args.file, layout, args.limit):
            for line, ts in zip(lines, ts_batch, strict=True):
                pacer.wait_for(ts)
                key, value = build_message(line, ts, layout.field_index, run_id)
                while True:
                    try:
                        producer.produce(args.topic, value, key, on_delivery=tracker.on_delivery)
                        break
                    except BufferError:  # local queue full: let librdkafka drain it
                        producer.poll(0.1)
                producer.poll(0)
                sent += 1
                if sent % 500_000 == 0:
                    rate = sent / (time.monotonic() - t0)
                    print(f"replayer: sent {sent:,} ({rate:,.0f}/s)", file=sys.stderr)
    except KeyboardInterrupt:
        status = "interrupted"
        print("replayer: interrupted, flushing what was already produced", file=sys.stderr)

    undelivered = producer.flush(FLUSH_TIMEOUT_S)
    if status == "complete" and (undelivered or tracker.errors or tracker.delivered != sent):
        status = "failed"
    elapsed = time.monotonic() - t0

    out = args.runs_dir / run_id / "produced.json"
    write_json_atomic(
        out,
        {
            "run_id": run_id,
            "status": status,
            "topic": args.topic,
            "source_file": str(args.file),
            "speed": args.speed,
            "limit": args.limit,
            "brokers": args.brokers,
            "started_at": started_at,
            "finished_at": utc_now(),
            "elapsed_s": round(elapsed, 3),
            "sent": sent,
            "undelivered": undelivered,
            **tracker.summary(),
        },
    )
    print(
        f"replayer: {status}: sent={sent:,} delivered={tracker.delivered:,} errors={tracker.errors} "
        f"undelivered={undelivered} in {elapsed:.1f}s, sleeps={pacer.sleep_calls} -> {out}",
        file=sys.stderr,
    )
    return {"complete": 0, "failed": 1}.get(status, 130)
