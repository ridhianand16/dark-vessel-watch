"""Consume ais.tracks and upsert the latest position per vessel into vessels_current.

    uv run python -m sinks.postgis_vessels     # Compose: vessels-sink

Per batch: map each track message to a VesselRow, keep one row per MMSI (the latest
event_ts), and upsert them in one statement inside one transaction. Offsets are
committed only after that transaction has committed (common/kafka_batch.py).

Idempotent by event time (ADR 0012): ON CONFLICT replaces a row only when the new
event_ts is strictly later. Re-delivering a batch, resetting the consumer group or
replaying an older run therefore writes nothing, and a row never moves back in time.
A track message that cannot be used is skipped and logged; its offset is committed,
because retrying it would fail the same way, and vessels_current is derived state
that the next accepted fix for that vessel corrects.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from confluent_kafka import Consumer

from common.kafka_batch import BatchSettings, BatchSink
from common.postgis import conninfo
from sinks.clickhouse.convert import BadValue, decode_object, to_float, ts_ms, u32

NAME = "vessels"
TOPIC = "ais.tracks"
GROUP_ID = "dvw-vessels-sink"
SCHEMA_VERSION = 1
MMSI_MAX = 999_999_999
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# vessels_current is not on the alert latency path; 1 s batches keep the statement count low.
SETTINGS = BatchSettings(max_rows=10_000, max_wait_s=1.0)

# One statement per batch: unnest() turns the parallel arrays into rows. Postgres
# rejects an INSERT ... ON CONFLICT that touches the same key twice, hence
# latest_per_mmsi() first. rowcount counts inserted plus updated rows, so the rows the
# WHERE guard kept out are len(rows) - rowcount.
UPSERT = """
INSERT INTO vessels_current AS v (mmsi, run_id, event_ts, position, sog, cog)
SELECT t.mmsi, t.run_id, t.event_ts, ST_SetSRID(ST_MakePoint(t.lon, t.lat), 4326), t.sog, t.cog
FROM unnest(%s::integer[], %s::text[], %s::timestamptz[],
            %s::float8[], %s::float8[], %s::float8[], %s::float8[])
     AS t(mmsi, run_id, event_ts, lon, lat, sog, cog)
ON CONFLICT (mmsi) DO UPDATE SET
    run_id = excluded.run_id,
    event_ts = excluded.event_ts,
    position = excluded.position,
    sog = excluded.sog,
    cog = excluded.cog,
    updated_at = now()
WHERE excluded.event_ts > v.event_ts
"""


@dataclass(frozen=True)
class VesselRow:
    mmsi: int
    run_id: str
    event_ts_ms: int
    lat: float
    lon: float
    sog: float
    cog: float


class BadTrack(ValueError):
    pass


def _field(obj: dict[str, Any], name: str, conv: Callable[[Any], Any]) -> Any:
    try:
        v = conv(obj.get(name))
    except BadValue:
        raise BadTrack(f"bad_field:{name}") from None
    if v is None:
        raise BadTrack(f"missing:{name}")
    return v


def parse_track(value: bytes | None) -> VesselRow:
    """The vessels_current row for one ais.tracks v1 value. Raises BadTrack with a reason."""
    obj, err, _ = decode_object(value)
    if obj is None:
        raise BadTrack(err)
    if obj.get("schema_version") != SCHEMA_VERSION:
        raise BadTrack("schema_version")
    mmsi = _field(obj, "mmsi", u32)
    if mmsi > MMSI_MAX:
        raise BadTrack("bad_field:mmsi")
    run_id = obj.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise BadTrack("bad_field:run_id")
    lat, lon = _field(obj, "lat", to_float), _field(obj, "lon", to_float)
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        raise BadTrack("bad_field:position")
    return VesselRow(
        mmsi=mmsi,
        run_id=run_id,
        event_ts_ms=_field(obj, "event_ts", ts_ms),
        lat=lat,
        lon=lon,
        sog=_field(obj, "sog_kn", to_float),
        cog=_field(obj, "cog_deg", to_float),
    )


def latest_per_mmsi(rows: Iterable[VesselRow]) -> list[VesselRow]:
    """One row per MMSI: the one with the latest event_ts.

    On a tie the first row received wins, the same rule the upsert's strict > applies
    across batches, so the stored row does not depend on where a batch boundary fell.
    Rows for one MMSI arrive in offset order (one key, one partition). The result is
    ordered by MMSI, which makes the statement deterministic and its row locks taken
    in a fixed order.
    """
    latest: dict[int, VesselRow] = {}
    for r in rows:
        cur = latest.get(r.mmsi)
        if cur is None or r.event_ts_ms > cur.event_ts_ms:
            latest[r.mmsi] = r
    return [latest[m] for m in sorted(latest)]


def upsert_params(rows: list[VesselRow]) -> tuple[list[Any], ...]:
    """UPSERT's parameters: one array per column, in the unnest() order."""
    return (
        [r.mmsi for r in rows],
        [r.run_id for r in rows],
        [EPOCH + timedelta(milliseconds=r.event_ts_ms) for r in rows],  # exact, unlike float seconds
        [r.lon for r in rows],
        [r.lat for r in rows],
        [r.sog for r in rows],
        [r.cog for r in rows],
    )


def upsert(conn: psycopg.Connection[Any], rows: list[VesselRow]) -> int:
    """Upsert rows (at most one per MMSI) in one transaction; returns rows written.

    Returns only after COMMIT has been acknowledged, so the caller may commit offsets.
    """
    with conn.transaction():
        cur = conn.execute(UPSERT, upsert_params(rows))
        return cur.rowcount


def connect() -> psycopg.Connection[Any]:
    # autocommit: transactions are explicit (conn.transaction()), so none stays open
    # between batches. statement_timeout keeps a stuck upsert inside the retry budget.
    return psycopg.connect(conninfo(), autocommit=True, connect_timeout=5, options="-c statement_timeout=30s")


class VesselsSink(BatchSink[VesselRow]):
    def __init__(
        self,
        consumer: Consumer,
        connect: Callable[[], psycopg.Connection[Any]] = connect,
        settings: BatchSettings = SETTINGS,
    ) -> None:
        super().__init__(NAME, TOPIC, consumer, settings)
        self._connect = connect
        self._conn: psycopg.Connection[Any] | None = None

    def to_row(self, value: bytes | None, partition: int, offset: int, kafka_ts_ms: int) -> VesselRow | None:
        try:
            return parse_track(value)
        except BadTrack as e:
            self.log.warning("skipping %s[%d]@%d: %s", TOPIC, partition, offset, e)
            return None

    def write(self, rows: list[VesselRow]) -> dict[str, int]:
        latest = latest_per_mmsi(rows)
        if self._conn is None or self._conn.closed or self._conn.broken:
            self._conn = self._connect()
        written = upsert(self._conn, latest)
        return {"vessels": len(latest), "written": written, "stale": len(latest) - written}

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
