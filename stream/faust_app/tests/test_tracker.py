from __future__ import annotations

import json
from typing import Any

import pytest

from stream.faust_app.kalman import KalmanConfig
from stream.faust_app.tracker import (
    RawRecord,
    Result,
    Skip,
    VesselState,
    apply,
    decode,
    process_batch,
)

CFG = KalmanConfig()
MMSI = 219026147
KEY = str(MMSI).encode()
T0 = 1_761_609_601_000


def value(**overrides: Any) -> bytes:
    msg: dict[str, Any] = {
        "schema_version": 1,
        "event_ts": T0,
        "mmsi": MMSI,
        "lat": 56.0,
        "lon": 10.0,
        "sog": 10.0,
        "cog": 90.0,
        "heading": 90,
        "raw": "...",
        "replay_send_ts": 1_762_090_021_000,
        "trace_id": "6f1c2a8e-1b7e-4c55-9d1e-2f3a4b5c6d7e",
        "run_id": "20261102T132701Z-6cdc0c",
    }
    msg.update(overrides)
    return json.dumps(msg).encode()


def rec(offset: int, key: bytes | None = KEY, partition: int = 3, **overrides: Any) -> RawRecord:
    return RawRecord(key, value(**overrides), partition, offset)


# --- decode -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("key", "val", "expected"),
    [
        (b"invalid", value(), Skip.INVALID_KEY),
        (None, value(), Skip.INVALID_KEY),
        (KEY, b"\xff\xfe", Skip.DECODE_ERROR),
        (KEY, b"not json", Skip.DECODE_ERROR),
        (KEY, b"[1, 2]", Skip.DECODE_ERROR),
        (KEY, None, Skip.DECODE_ERROR),
        (KEY, value(schema_version=2), Skip.DECODE_ERROR),
        (b"219000000", value(), Skip.DECODE_ERROR),  # key and value disagree
        (KEY, value(run_id=None), Skip.DECODE_ERROR),
        (b"992191234", value(mmsi=992191234), Skip.NOT_A_SHIP),  # aid to navigation
        (b"2190047", value(mmsi=2190047, event_ts=None), Skip.NOT_A_SHIP),  # before no_event_ts
        (KEY, value(event_ts=None), Skip.NO_EVENT_TS),
        (KEY, value(lat=None), Skip.NO_POSITION),
        (KEY, value(lat=91.0, lon=181.0), Skip.NO_POSITION),  # AIS "not available"
        (KEY, value(lon=True), Skip.NO_POSITION),  # bool is not a number here
    ],
)
def test_decode_skips(key: bytes | None, val: bytes | None, expected: Skip) -> None:
    assert decode(key, val) == expected


def test_decode_fix() -> None:
    fix = decode(KEY, value(sog=None))
    assert not isinstance(fix, Skip)
    assert (fix.mmsi, fix.event_ts, fix.lat, fix.lon, fix.sog) == (MMSI, T0, 56.0, 10.0, None)


# --- apply ----------------------------------------------------------------------


def run(*records: RawRecord) -> tuple[VesselState | None, list[Result], list[dict[str, Any]]]:
    state: VesselState | None = None
    results: list[Result] = []
    tracks: list[dict[str, Any]] = []
    for r in records:
        fix = decode(r.key, r.value)
        assert not isinstance(fix, Skip)
        out = apply(state, fix, r.partition, r.offset, CFG)
        results.append(out.result)
        if out.state is not None:
            state = out.state
        if out.track is not None:
            tracks.append(json.loads(out.track))
    return state, results, tracks


def test_first_fix_starts_track_without_innovation() -> None:
    state, results, tracks = run(rec(10))
    assert results == [Result.STARTED]
    t = tracks[0]
    assert (t["lat"], t["lon"]) == pytest.approx((56.0, 10.0))
    assert t["sog_kn"] == pytest.approx(10.0)
    assert t["cog_deg"] == pytest.approx(90.0)
    assert t["innovation_m"] is None and t["dt_s"] is None
    assert (t["fix_seq"], t["src_partition"], t["src_offset"]) == (1, 3, 10)
    assert state is not None and state.last_offset == 10


def test_update_moves_along_track() -> None:
    # 10 kn east for 60 s is about 309 m; at 56N that is 0.00497 degrees of longitude.
    _, results, tracks = run(rec(10), rec(11, event_ts=T0 + 60_000, lon=10.00497))
    assert results == [Result.STARTED, Result.UPDATED]
    t = tracks[1]
    assert t["dt_s"] == 60.0
    assert t["fix_seq"] == 2
    assert t["innovation_m"] < 10.0  # the fix is where SOG/COG said it would be
    assert t["lon"] == pytest.approx(10.00497, abs=1e-4)


def test_new_run_id_resets_state() -> None:
    state, results, tracks = run(
        rec(10),
        rec(11, event_ts=T0 + 60_000, lon=10.00497),
        rec(500, event_ts=T0 - 3_600_000, lat=55.0, lon=9.0, run_id="20261102T140540Z-fe1019"),
    )
    assert results == [Result.STARTED, Result.UPDATED, Result.RESTARTED]
    assert state is not None
    assert (state.run_id, state.anchor_lat, state.anchor_lon, state.fix_seq) == (
        "20261102T140540Z-fe1019",
        55.0,
        9.0,
        1,
    )
    assert tracks[2]["innovation_m"] is None


def test_out_of_order_is_counted_and_ignored() -> None:
    state, results, tracks = run(rec(10), rec(11, event_ts=T0 + 60_000, lon=10.00497), rec(12, event_ts=T0 + 30_000))
    assert results == [Result.STARTED, Result.UPDATED, Result.OUT_OF_ORDER]
    assert len(tracks) == 2
    assert state is not None and state.last_offset == 11 and state.last_event_ts == T0 + 60_000


def test_exact_duplicate_is_ignored_but_same_time_new_position_is_not() -> None:
    _, results, _ = run(rec(10), rec(11), rec(12, lon=10.0001))
    assert results == [Result.STARTED, Result.DUPLICATE, Result.UPDATED]


def test_redelivered_offset_is_ignored() -> None:
    state, results, tracks = run(rec(10), rec(11, event_ts=T0 + 60_000, lon=10.00497), rec(11), rec(10))
    assert results[2:] == [Result.REDELIVERED, Result.REDELIVERED]
    assert len(tracks) == 2


def test_state_round_trips() -> None:
    state, _, _ = run(rec(10), rec(11, event_ts=T0 + 60_000, lon=10.00497))
    assert state is not None
    back = VesselState.from_bytes(state.to_bytes())
    assert back.to_bytes() == state.to_bytes()
    with pytest.raises(ValueError):
        VesselState.from_bytes(b'{"v": 99}')


# --- process_batch --------------------------------------------------------------


class Store:
    """Stands in for the Faust table."""

    def __init__(self) -> None:
        self.data: dict[bytes, bytes] = {}
        self.loads: list[bytes] = []

    def apply(self, records: list[RawRecord]) -> tuple[list[tuple[bytes, bytes]], dict[str, int]]:
        def load(i: int) -> bytes | None:
            key = records[i].key
            assert key is not None
            self.loads.append(key)
            return self.data.get(key)

        result = process_batch(records, load, CFG)
        for _, key, blob in result.writes:
            self.data[key] = blob
        return result.tracks, dict(result.counts)


OTHER = b"257000000"


def batch() -> list[RawRecord]:
    return [
        rec(10),
        rec(0, key=OTHER, partition=1, mmsi=257000000, lat=60.0, lon=5.0),
        rec(11, event_ts=T0 + 60_000, lon=10.00497),
        rec(12, key=b"invalid", mmsi=None),
        RawRecord(KEY, b"garbage", 3, 13),
        rec(14, event_ts=T0 + 60_000, lon=10.00497),  # duplicate of offset 11
        rec(15, event_ts=T0 + 120_000, lon=10.00994),
    ]


def test_batch_counts_outputs_and_writes() -> None:
    store = Store()
    tracks, counts = store.apply(batch())
    assert counts == {
        "read": 7,
        "started": 2,
        "updated": 2,
        "duplicate": 1,
        "invalid_key": 1,
        "decode_error": 1,
    }
    assert [k for k, _ in tracks] == [KEY, OTHER, KEY, KEY]
    assert sorted(store.loads) == sorted([KEY, OTHER])  # once per key
    assert VesselState.from_bytes(store.data[KEY]).last_offset == 15


BUOY = b"992191234"  # 99: aid to navigation


def test_buoy_gets_no_track_and_is_counted() -> None:
    store = Store()
    records = [
        rec(20, key=BUOY, mmsi=992191234),
        rec(21, key=BUOY, mmsi=992191234, event_ts=T0 + 60_000),
        rec(22),
    ]
    tracks, counts = store.apply(records)
    assert [k for k, _ in tracks] == [KEY]
    assert BUOY not in store.data and BUOY not in store.loads
    assert counts == {"read": 3, "not_a_ship": 2, "started": 1}


def test_counts_add_up_to_read() -> None:
    _, counts = Store().apply(batch())
    per_record = {s.value for s in Skip} | {r.value for r in Result}
    assert sum(v for k, v in counts.items() if k in per_record) == counts["read"]


def test_redelivery_after_crash_is_exact_or_skipped() -> None:
    """The two crash cases in ADR 0004.

    State not yet written: the batch runs again from the old state and produces the
    same bytes. State written: every record is behind it and is skipped.
    """
    first = Store()
    tracks_1, _ = first.apply(batch())

    not_written = Store()
    tracks_2, _ = not_written.apply(batch())
    assert tracks_2 == tracks_1

    tracks_3, counts_3 = first.apply(batch())
    assert tracks_3 == []
    assert counts_3["redelivered"] == 5
