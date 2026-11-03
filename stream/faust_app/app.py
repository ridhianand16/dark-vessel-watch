"""Faust worker: ais.raw -> per-MMSI Kalman tracks -> ais.tracks, and alerts -> ais.alerts.

    uv run faust -A stream.faust_app.app worker -l info   # normally runs in Compose as tracker

Faust in two lines: an *agent* is an async function fed by a Kafka topic's stream, and
a *table* is a dict whose writes also go to a compacted changelog topic. The changelog
is partitioned like the source topic, so whoever owns ais.raw partition 3 also owns
the vessel states for partition 3, and can rebuild them from the changelog after a
restart. RocksDB is just the local on-disk copy.

Delivery (ADR 0004): at-least-once, and outputs before state. Records are taken in
batches. For each batch: compute tracks and new states in memory (tracker.py), send
the tracks and wait for the broker to acknowledge them, and only then write the
states. Faust commits offsets later, after flushing the producer. A crash at any
point therefore leaves state at or behind what ais.tracks already holds. Records
behind the state are skipped as `redelivered`; records ahead of it are processed
again and produce byte-identical messages, which sinks dedup on
(src_partition, src_offset).

Detections (ADR 0005) follow the same order. TELEPORT, and DARK_GAP for a vessel that
reports again, come out of the batch with the tracks. DARK_GAP for vessels still
silent comes from a scan at the end of a batch, whenever a partition's watermark
(max event_ts per partition and run) enters a new DARK_SCAN_STEP_S step: send the
alerts, wait for the acks, then write gap_alerted. The scan runs here and not in a
Faust timer: a timer cannot write the table (Faust raises outside stream iteration),
and would race with this read-send-write on the same keys. The watermark only moves
when records arrive, so a timer between batches would see nothing new anyway.

Any unexpected error stops the process at once (exit 70) without committing. Faust
would otherwise log it, ack the batch, and carry on, which drops those records.
"""

from __future__ import annotations

import asyncio
import logging
import os
import weakref
from collections import Counter
from collections.abc import Awaitable, Iterator, Sequence
from contextlib import contextmanager

import faust
import faust.streams
from faust.types import EventT

from stream.faust_app import config
from stream.faust_app.detect import Watermarks
from stream.faust_app.tracker import ACCEPTED, RawRecord, process_batch, scan_dark_gaps

APP_ID = "dvw-tracker"
EXIT_CRASH = 70  # EX_SOFTWARE
PARTITIONS = 6

log = logging.getLogger("tracker")
cfg = config.load()

app = faust.App(
    APP_ID,
    broker=f"kafka://{cfg.brokers}",
    store="rocksdb://",
    datadir=cfg.data_dir,
    topic_partitions=PARTITIONS,
    topic_replication_factor=1,
    processing_guarantee="at_least_once",
    consumer_auto_offset_reset="earliest",
    producer_acks=-1,
    web_enabled=False,
)

raw_topic = app.topic(
    "ais.raw",
    key_type=bytes,
    value_type=bytes,
    key_serializer="raw",
    value_serializer="raw",
    partitions=PARTITIONS,
    internal=False,
)
tracks_topic = app.topic(
    "ais.tracks",
    key_type=bytes,
    value_type=bytes,
    key_serializer="raw",
    value_serializer="raw",
    partitions=PARTITIONS,
    internal=False,
)
alerts_topic = app.topic(
    "ais.alerts",
    key_type=bytes,
    value_type=bytes,
    key_serializer="raw",
    value_serializer="raw",
    partitions=PARTITIONS,
    internal=False,
)

# Faust's RocksDB defaults are a 2 GB block cache (+500 MB compressed) per partition
# DB. The state here is ~0.5 kB per vessel, a few MB in total, so keep them small.
# key_type/value_type bytes means the "raw" codec: the changelog key is the same
# str(mmsi) bytes as the ais.raw key.
vessels = app.Table(
    "vessel_state",
    key_type=bytes,
    value_type=bytes,
    partitions=PARTITIONS,
    options={
        "block_cache_size": 8 * 1024**2,
        "block_cache_compressed_size": 8 * 1024**2,
        "write_buffer_size": 4 * 1024**2,
        "max_write_buffer_number": 2,
    },
)

counts: Counter[str] = Counter()
watermarks = Watermarks(cfg.detect)


@contextmanager
def as_current_event(event: EventT[bytes]) -> Iterator[None]:
    """Make `event` Faust's current event while reading or writing the table.

    Faust picks the table's changelog partition and local RocksDB partition from the
    current event. Inside take_events() that is the batch's *last* event, which may
    be in another partition than the key being written. Private API: covered by
    test_app.py, so a Faust upgrade that changes it fails the tests.
    """
    token = faust.streams._current_event.set(weakref.ref(event))
    try:
        yield
    finally:
        faust.streams._current_event.reset(token)


async def handle(events: Sequence[EventT[bytes]]) -> None:
    records = [RawRecord(e.message.key, e.message.value, e.message.partition, e.message.offset) for e in events]

    def load(i: int) -> bytes | None:
        with as_current_event(events[i]):
            blob: bytes | None = vessels.get(records[i].key)
            return blob

    result = process_batch(records, load, cfg.kalman, cfg.detect)
    acks = [await tracks_topic.send(key=k, value=v) for k, v in result.tracks]
    acks += [await alerts_topic.send(key=k, value=v) for k, v in result.alerts]
    await asyncio.gather(*acks)  # raises if any send failed: state must not move past it
    for i, key, blob in result.writes:
        with as_current_event(events[i]):
            vessels[key] = blob
    counts.update(result.counts)

    due = watermarks.advance(result.seen.items())
    if due:
        await scan(events, due)


async def scan(events: Sequence[EventT[bytes]], partitions: set[int]) -> None:
    # Table iteration covers actively assigned partitions only (Faust's RocksDB store
    # skips standby replicas), and scan_dark_gaps() keeps just `partitions`: those
    # whose watermark moved, all of them in this batch.
    result = scan_dark_gaps(vessels.items(), watermarks, partitions, cfg.kalman, cfg.detect)
    acks: list[Awaitable[object]] = [await alerts_topic.send(key=k, value=v) for k, v in result.alerts]
    await asyncio.gather(*acks)
    # Writing needs a current event in the vessel's partition (see as_current_event).
    event_in = {e.message.partition: e for e in events}
    for partition, key, blob in result.writes:
        with as_current_event(event_in[partition]):
            vessels[key] = blob
    counts["scanned"] += result.scanned
    counts["alert_dark_gap"] += len(result.alerts)


async def handle_or_exit(events: Sequence[EventT[bytes]]) -> None:
    try:
        await handle(events)
    except Exception:
        log.critical("batch failed; exiting without committing", exc_info=True)
        logging.shutdown()
        os._exit(EXIT_CRASH)


@app.agent(raw_topic, concurrency=1)
async def track(stream: faust.StreamT[bytes]) -> None:
    assert isinstance(stream, faust.Stream)  # StreamT (the protocol) lacks take_events
    # concurrency=1: one actor reads all assigned partitions in order, so each
    # vessel's records are handled in offset order.
    async for events in stream.take_events(cfg.batch_max, within=cfg.batch_within_s):
        task = asyncio.ensure_future(handle_or_exit(events))
        try:
            # Shielded: on shutdown take_events() acks the batch as soon as this body
            # exits, so finish it first. Otherwise offsets could be committed for
            # records whose state was never written.
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise


async def log_counts() -> None:
    accepted = sum(counts[r.value] for r in ACCEPTED)
    log.info("tracks=%d %s", accepted, " ".join(f"{k}={v}" for k, v in sorted(counts.items())))


# Not decorator syntax: app.timer is untyped, which mypy --strict rejects on a decorator.
app.timer(interval=30.0)(log_counts)
