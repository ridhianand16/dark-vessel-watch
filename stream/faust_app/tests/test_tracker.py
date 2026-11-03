from __future__ import annotations

import json
from typing import Any

import pytest

from stream.faust_app.kalman import KalmanConfig
from stream.faust_app.mmsi import StationClass
from stream.faust_app.tracker import (
    LEAK_SKIPS,
    Leak,
    PartitionMismatch,
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


# What the Flink job adds on ais.enriched. The default position is 3 km from a
# protected area, so DARK_GAP's zone condition holds.
ZONE: dict[str, Any] = {
    "station_class": "ship",
    "zones_version": "test-1",
    "eez_id": "eez:5674",
    "mpa_ids": [],
    "in_port": False,
    "dist_eez_boundary_m": 40_000.0,
    "dist_mpa_m": 3_000.0,
}


def erec(offset: int, key: bytes | None = KEY, partition: int = 3, **overrides: Any) -> RawRecord:
    """An ais.enriched record for ais.raw `offset`. Its own offset in ais.enriched is
    smaller, as it would be after the Flink job drops records: never the bookmark."""
    fields = {**ZONE, "src_partition": partition, "src_offset": offset, **overrides}
    return RawRecord(key, value(**fields), partition, offset // 2)


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


def test_duplicate_compares_positions_rounded_like_flink() -> None:
    """Same second, 0.2 m east: the same report to the Flink job's dedup (5 decimals), so
    a duplicate here too. 1.1 m east rounds to another key and is a new fix."""
    _, results, _ = run(rec(10, lon=10.000001), rec(11, lon=10.000004), rec(12, lon=10.00002))
    assert results == [Result.STARTED, Result.DUPLICATE, Result.UPDATED]


def test_duplicate_rounding_sends_halves_up_like_java() -> None:
    """55.000005 rounds up to 5500001 in Java, so it is not the same report as 55.0;
    Python's round() would have said 5500000, a duplicate."""
    _, results, _ = run(rec(10, lat=55.0), rec(11, lat=55.000005))
    assert results == [Result.STARTED, Result.UPDATED]


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

    def apply(
        self, records: list[RawRecord], enriched: bool = False
    ) -> tuple[list[tuple[bytes, bytes]], dict[str, int]]:
        def load(i: int) -> bytes | None:
            key = records[i].key
            assert key is not None
            self.loads.append(key)
            return self.data.get(key)

        result = process_batch(records, load, CFG, enriched=enriched)
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


# --- ais.enriched ---------------------------------------------------------------


@pytest.mark.parametrize(
    "missing", ["src_partition", "src_offset", "station_class", "zones_version", "mpa_ids", "in_port"]
)
def test_enriched_record_without_flink_fields_is_a_decode_error(missing: str) -> None:
    overrides: dict[str, Any] = {missing: None}
    r = erec(10, **overrides)
    assert decode(r.key, r.value, enriched=True) == Skip.DECODE_ERROR
    assert not isinstance(decode(r.key, r.value), Skip)  # read as ais.raw it is fine


def test_enriched_fix_carries_source_and_zone() -> None:
    r = erec(10, dist_eez_boundary_m=None)
    fix = decode(r.key, r.value, enriched=True)
    assert not isinstance(fix, Skip) and fix.zone is not None
    assert (fix.src_partition, fix.src_offset) == (3, 10)
    assert (fix.zone.dist_mpa_m, fix.zone.dist_eez_boundary_m, fix.zone.in_port) == (3000.0, None, False)


def enriched_batch(records: list[RawRecord]) -> tuple[list[dict[str, object]], dict[str, int], VesselState | None]:
    result = process_batch(records, lambda i: None, CFG, enriched=True)
    state = {k: b for _, k, b in result.writes}.get(KEY)
    tracks = [json.loads(v) for _, v in result.tracks]
    return tracks, dict(result.counts), VesselState.from_bytes(state) if state is not None else None


def test_enriched_bookmark_is_the_raw_offset() -> None:
    """erec() gives each record an enriched offset half its raw one. Tracks and the
    bookmark must use the raw offset."""
    tracks, _, state = enriched_batch([erec(10), erec(11, event_ts=T0 + 60_000, lon=10.00497)])
    assert [(t["src_partition"], t["src_offset"]) for t in tracks] == [(3, 10), (3, 11)]
    assert state is not None and state.last_offset == 11


def test_enriched_tracks_equal_raw_tracks() -> None:
    """The same records through either input give byte-identical tracks."""
    records: list[tuple[int, dict[str, Any]]] = [
        (10, {}),
        (11, {"event_ts": T0 + 60_000, "lon": 10.00497}),
        (12, {"event_ts": T0 + 120_000, "lon": 10.01}),
    ]
    raw = process_batch([rec(o, **kw) for o, kw in records], lambda i: None, CFG)
    enr = process_batch([erec(o, **kw) for o, kw in records], lambda i: None, CFG, enriched=True)
    assert raw.tracks == enr.tracks


def test_enriched_bookmark_skips_what_was_applied_from_raw() -> None:
    """A vessel state built from ais.raw, then the same run read from ais.enriched:
    every record is behind the bookmark (enriched offsets alone would not be)."""
    raw = process_batch([rec(10), rec(11, event_ts=T0 + 60_000, lon=10.00497)], lambda i: None, CFG)
    blob = raw.writes[0][2]
    again = process_batch(
        [erec(10), erec(11, event_ts=T0 + 60_000, lon=10.00497), erec(12, event_ts=T0 + 120_000, lon=10.01)],
        lambda i: blob, CFG, enriched=True,
    )
    assert again.counts["redelivered"] == 2 and again.counts["updated"] == 1


def test_leaks_are_counted() -> None:
    _, counts, _ = enriched_batch([
        erec(10),
        erec(11),  # duplicate of 10: Flink should have dropped it
        erec(12, lat=None),  # Flink sends this to ais.dlq
        erec(13, key=BUOY, mmsi=992191234, station_class="aid_to_navigation"),  # valid, skipped, no leak
        erec(14, key=BUOY, mmsi=992191234, station_class="ship"),  # Flink and mmsi.py disagree
    ])
    assert counts[Leak.DUPLICATE.value] == 1
    assert counts[Leak.INVALID.value] == 1
    assert counts[Leak.STATION_CLASS.value] == 1
    assert counts["not_a_ship"] == 1 and counts["station_class_mismatch"] == 1
    per_record = {s.value for s in Skip} | {r.value for r in Result}
    assert sum(v for k, v in counts.items() if k in per_record) == counts["read"] == 5


# One enriched record per skip that Flink should have made impossible.
LEAK_SKIP_RECORDS: list[tuple[RawRecord, Skip]] = [
    (erec(10, key=b"invalid"), Skip.INVALID_KEY),
    (erec(10, src_offset=None), Skip.DECODE_ERROR),
    (erec(10, event_ts=None), Skip.NO_EVENT_TS),
    (erec(10, lat=None), Skip.NO_POSITION),
]


def test_leak_skip_cases_cover_every_leak_skip() -> None:
    assert {skip for _, skip in LEAK_SKIP_RECORDS} == LEAK_SKIPS


@pytest.mark.parametrize(("record", "skip"), LEAK_SKIP_RECORDS)
def test_every_invalid_skip_on_enriched_is_one_leak(record: RawRecord, skip: Skip) -> None:
    _, counts, state = enriched_batch([record])
    assert counts == {"read": 1, skip.value: 1, Leak.INVALID.value: 1}
    assert state is None


CRAFT_KEY = b"982190001"


@pytest.mark.parametrize(
    ("key", "mmsi", "tagged"),
    [
        (KEY, MMSI, "craft_with_parent"),  # ship tagged as the other ship class
        (CRAFT_KEY, 982190001, "ship"),  # and the reverse
        (BUOY, 992191234, "ship"),  # non-ship tagged ship
        (KEY, MMSI, "aid_to_navigation"),  # ship tagged non-ship
        (BUOY, 992191234, "coast"),  # both non-ship, still not the same class
        (KEY, MMSI, "aton"),  # not a wire value at all
    ],
)
def test_class_mismatch_on_enriched_is_skipped_and_counted(key: bytes, mmsi: int, tagged: str) -> None:
    tracks, counts, _ = enriched_batch([erec(10, key=key, mmsi=mmsi, station_class=tagged)])
    assert tracks == []
    assert counts == {"read": 1, "station_class_mismatch": 1, Leak.STATION_CLASS.value: 1}


@pytest.mark.parametrize("tagged", [*(c.value for c in StationClass), "aton"])
@pytest.mark.parametrize(("key", "mmsi"), [(KEY, MMSI), (CRAFT_KEY, 982190001), (BUOY, 992191234)])
def test_class_mismatch_is_unreachable_on_raw(key: bytes, mmsi: int, tagged: str) -> None:
    """decode() only reads station_class on the enriched path. On ais.raw, a record that
    happens to carry any station_class decodes by mmsi.py alone: never a mismatch, never
    a leak."""
    r = rec(10, key=key, mmsi=mmsi, station_class=tagged)
    assert decode(r.key, r.value) != Skip.CLASS_MISMATCH
    _, counts = Store().apply([r])
    assert "station_class_mismatch" not in counts
    assert not any(k.startswith("leak_") for k in counts)


def test_no_leaks_counted_on_raw_input() -> None:
    _, counts = Store().apply(batch())
    assert not any(k.startswith("leak_") for k in counts)


def test_enriched_partition_must_match_raw() -> None:
    r = erec(10)
    moved = RawRecord(r.key, r.value, 4, r.offset)
    with pytest.raises(PartitionMismatch):
        process_batch([moved], lambda i: None, CFG, enriched=True)


CRAFT = 982190001  # 98: craft associated with a parent ship, a ship station


def test_enriched_craft_with_parent_is_tracked() -> None:
    """Flink tags 98-prefix MMSIs craft_with_parent, not ship. That is a ship class, so
    the record is tracked and no station-class leak is counted."""
    key = str(CRAFT).encode()
    result = process_batch(
        [erec(10, key=key, mmsi=CRAFT, station_class="craft_with_parent")], lambda i: None, CFG, enriched=True
    )
    assert [k for k, _ in result.tracks] == [key]
    assert result.counts["started"] == 1
    assert result.counts[Leak.STATION_CLASS.value] == 0
    assert result.counts["station_class_mismatch"] == 0


# --- ais.enriched: crash and redelivery (ADR 0004 with the src_offset bookmark) ----


def enriched_records() -> list[RawRecord]:
    return [
        erec(10),
        erec(11, event_ts=T0 + 60_000, lon=10.00497),
        erec(12, event_ts=T0 + 120_000, lon=10.00994),
    ]


def test_enriched_redelivery_after_crash_is_exact_or_skipped() -> None:
    """The two crash cases in ADR 0004 on ais.enriched. State not yet written: the batch
    runs again from the old state and gives the same bytes. State written but offsets
    not committed: every record comes again and is behind the src_offset bookmark."""
    first = Store()
    tracks_1, _ = first.apply(enriched_records(), enriched=True)
    assert [json.loads(v)["src_offset"] for _, v in tracks_1] == [10, 11, 12]

    not_written = Store()
    tracks_2, _ = not_written.apply(enriched_records(), enriched=True)
    assert tracks_2 == tracks_1

    tracks_3, counts_3 = first.apply(enriched_records(), enriched=True)
    assert tracks_3 == []
    assert counts_3 == {"read": 3, "redelivered": 3}
    assert VesselState.from_bytes(first.data[KEY]).last_offset == 12


def test_flink_rewrite_at_new_enriched_offsets_is_skipped_by_src_offset() -> None:
    """Flink recovery writes records again at new ais.enriched offsets (their first,
    aborted, copies are never read). The bookmark is src_offset, so the new copies are
    behind it whatever their own offset, and only the record after them is applied."""
    store = Store()
    store.apply(enriched_records()[:2], enriched=True)
    rewritten = [RawRecord(r.key, r.value, r.partition, r.offset + 1_000) for r in enriched_records()[:2]]
    tracks, counts = store.apply([*rewritten, enriched_records()[2]], enriched=True)
    assert counts == {"read": 3, "redelivered": 2, "updated": 1}
    assert [json.loads(v)["src_offset"] for _, v in tracks] == [12]


def test_partition_mismatch_stops_the_batch_before_any_output() -> None:
    """A record in the wrong partition, after good ones in the same batch: the whole
    batch raises, so app.py sends nothing and writes no state for any of it."""
    good = enriched_records()[:2]
    moved = RawRecord(KEY, enriched_records()[2].value, 4, 7)
    store = Store()
    with pytest.raises(PartitionMismatch, match="ais.enriched/4@7 came from ais.raw/3"):
        store.apply([*good, moved], enriched=True)
    assert store.data == {}


def test_partition_is_not_checked_on_raw() -> None:
    """ais.raw records carry no src_partition; a src_partition field is ignored there."""
    _, counts = Store().apply([rec(10, src_partition=5, src_offset=99)])
    assert counts == {"read": 1, "started": 1}
