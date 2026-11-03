"""vessels_current sink: mapping, per-MMSI dedup, commit ordering, and the upsert guard
against a scratch database on the compose PostGIS (skipped when it is not running)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import psycopg
import pytest
from confluent_kafka import Consumer, TopicPartition

from common.kafka_batch import BatchSettings, WriteFailed
from common.postgis import conninfo
from sinks.postgis_vessels.sink import BadTrack, VesselRow, VesselsSink, connect, latest_per_mmsi, parse_track
from sql.migrate import main as migrate

T0 = 1_761_609_600_000  # 2025-10-28T00:00:00Z


def track(mmsi: int = 219000001, event_ts: int = T0, lat: float = 56.0, **extra: Any) -> bytes:
    v: dict[str, Any] = {
        "schema_version": 1, "mmsi": mmsi, "run_id": "r1", "event_ts": event_ts,
        "lat": lat, "lon": 11.0, "sog_kn": 8.5, "cog_deg": 270.0,
    }
    v.update(extra)
    return json.dumps(v).encode()


class FakeMessage:
    def __init__(self, partition: int, offset: int, value: bytes) -> None:
        self._p, self._o, self._v = partition, offset, value

    def error(self) -> None:
        return None

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o

    def timestamp(self) -> tuple[int, int]:
        return 1, T0

    def value(self) -> bytes:
        return self._v


class FakeConsumer:
    def __init__(self, batches: list[list[FakeMessage]], events: list[str]) -> None:
        self.batches, self.events = batches, events
        self.commits: list[list[tuple[int, int]]] = []

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets: list[TopicPartition], asynchronous: bool) -> None:
        assert asynchronous is False
        self.events.append("offsets")
        self.commits.append([(tp.partition, tp.offset) for tp in offsets])


class FakeCursor:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class FakeConn:
    """Records BEGIN/COMMIT around execute(), like psycopg's conn.transaction()."""

    closed = False
    broken = False

    def __init__(self, events: list[str], fail: bool = False) -> None:
        self.events, self.fail = events, fail
        self.params: list[tuple[list[Any], ...]] = []

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.events.append("begin")
        yield
        self.events.append("commit")

    def execute(self, query: str, params: tuple[list[Any], ...]) -> FakeCursor:
        if self.fail:
            raise psycopg.OperationalError("postgis down")
        self.params.append(params)
        return FakeCursor(len(params[0]))


def make(batches: list[list[FakeMessage]], fail: bool = False) -> tuple[VesselsSink, FakeConsumer, FakeConn]:
    events: list[str] = []
    consumer, conn = FakeConsumer(batches, events), FakeConn(events, fail)
    s = BatchSettings(max_rows=10, max_wait_s=0.01, write_attempts=2, backoff_base_s=0.0)
    sink = VesselsSink(cast(Consumer, consumer), lambda: cast(psycopg.Connection[Any], conn), s)
    return sink, consumer, conn


def row(mmsi: int, event_ts: int, lat: float = 56.0) -> VesselRow:
    return VesselRow(mmsi, "r1", event_ts, lat, 11.0, 8.5, 270.0)


# --- mapping -----------------------------------------------------------------------


def test_parses_a_track() -> None:
    assert parse_track(track()) == VesselRow(219000001, "r1", T0, 56.0, 11.0, 8.5, 270.0)


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (b"", "empty"),
        (b"{", "not_json"),
        (track(schema_version=2), "schema_version"),
        (track(mmsi=1_000_000_000), "bad_field:mmsi"),
        (track(run_id=""), "bad_field:run_id"),
        (track(lat=91.0), "bad_field:position"),
        (track(sog_kn=None), "missing:sog_kn"),
        (track(sog_kn="fast"), "bad_field:sog_kn"),
    ],
)
def test_rejects_unusable_tracks(value: bytes, reason: str) -> None:
    with pytest.raises(BadTrack, match=reason):
        parse_track(value)


# --- dedup -------------------------------------------------------------------------


def test_same_mmsi_three_times_out_of_order_keeps_the_latest() -> None:
    rows = [row(1, T0 + 2000, lat=56.2), row(1, T0, lat=56.0), row(1, T0 + 1000, lat=56.1)]
    assert latest_per_mmsi(rows) == [row(1, T0 + 2000, lat=56.2)]


def test_tie_keeps_the_first_received_and_output_is_ordered_by_mmsi() -> None:
    rows = [row(9, T0), row(1, T0, lat=1.0), row(1, T0, lat=2.0)]
    assert latest_per_mmsi(rows) == [row(1, T0, lat=1.0), row(9, T0)]


# --- delivery ----------------------------------------------------------------------


def test_one_statement_per_batch_then_offsets_after_commit() -> None:
    msgs = [FakeMessage(0, 5, track(event_ts=T0 + 2000)), FakeMessage(0, 6, track(event_ts=T0)),
            FakeMessage(3, 9, track(mmsi=7))]
    sink, consumer, conn = make([msgs])
    sink._collect()
    sink.flush()
    assert consumer.events == ["begin", "commit", "offsets"]
    assert conn.params[0][0] == [7, 219000001]  # one row per MMSI
    assert consumer.commits == [[(0, 7), (3, 10)]]


def test_unusable_track_is_skipped_but_its_offset_committed() -> None:
    sink, consumer, conn = make([[FakeMessage(2, 4, b"{")]])
    sink._collect()
    sink.flush()
    assert conn.params == []
    assert consumer.events == ["offsets"] and consumer.commits == [[(2, 5)]]


def test_never_commits_offsets_when_the_upsert_keeps_failing() -> None:
    sink, consumer, _ = make([[FakeMessage(1, 0, track())]], fail=True)
    sink._collect()
    with pytest.raises(WriteFailed):
        sink.flush()
    assert "offsets" not in consumer.events


# --- against PostGIS ---------------------------------------------------------------


@pytest.fixture
def db(scratch_db: str) -> Iterator[psycopg.Connection[Any]]:
    assert migrate(["--target", "postgis"]) == 0
    with psycopg.connect(conninfo(), autocommit=True) as conn:
        yield conn


def stored(db: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    return db.execute(
        "SELECT mmsi, run_id, (extract(epoch FROM event_ts) * 1000)::bigint, ST_Y(position), ST_X(position),"
        " sog, cog, updated_at FROM vessels_current ORDER BY mmsi"
    ).fetchall()


def write(values: list[bytes]) -> dict[str, int]:
    sink = VesselsSink(cast(Consumer, None), connect)
    try:
        return sink.write([parse_track(v) for v in values])
    finally:
        sink.close()


def test_postgis_three_out_of_order_fixes_upsert_once_and_store_the_latest(db: psycopg.Connection[Any]) -> None:
    counters = write([track(event_ts=T0 + 2000, lat=56.2), track(event_ts=T0, lat=56.0),
                      track(event_ts=T0 + 1000, lat=56.1)])
    assert counters == {"vessels": 1, "written": 1, "stale": 0}
    [(mmsi, run_id, ts, lat, lon, sog, cog, _)] = stored(db)
    assert (mmsi, run_id, ts, lat, lon, sog, cog) == (219000001, "r1", T0 + 2000, 56.2, 11.0, 8.5, 270.0)


def test_postgis_older_or_repeated_fix_changes_nothing(db: psycopg.Connection[Any]) -> None:
    write([track(event_ts=T0 + 2000, lat=56.2)])
    before = stored(db)
    assert write([track(event_ts=T0 + 2000, lat=56.2)]) == {"vessels": 1, "written": 0, "stale": 1}
    assert write([track(event_ts=T0, lat=55.0, run_id="older")]) == {"vessels": 1, "written": 0, "stale": 1}
    assert stored(db) == before  # updated_at included: the row was not rewritten


def test_postgis_later_fix_moves_the_row_forward(db: psycopg.Connection[Any]) -> None:
    write([track(event_ts=T0, lat=56.0)])
    assert write([track(event_ts=T0 + 1, lat=56.5, run_id="r2")])["written"] == 1
    [(_, run_id, ts, lat, *_)] = stored(db)
    assert (run_id, ts, lat) == ("r2", T0 + 1, 56.5)
