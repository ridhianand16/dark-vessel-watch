from __future__ import annotations

import json
from typing import Any

import pytest

from sinks.clickhouse.tracks import COLUMNS, INVALID_MMSI, to_row

KAFKA_TS = 1_761_609_601_500


def track(**overrides: Any) -> dict[str, Any]:
    """An ais.tracks v1 value as stream/faust_app/tracker.encode_track() writes it."""
    msg: dict[str, Any] = {
        "schema_version": 1,
        "mmsi": 219000001,
        "run_id": "20261103T120000Z-abc123",
        "event_ts": 1_761_609_600_000,
        "lat": 56.4,
        "lon": 11.8,
        "vx_ms": 1.5,
        "vy_ms": -0.5,
        "sog_kn": 3.07,
        "cog_deg": 108.4,
        "pos_sigma_m": 12.5,
        "meas_lat": 56.40001,
        "meas_lon": 11.80002,
        "innovation_m": 4.2,
        "innovation_nis": 0.3,
        "dt_s": 10.0,
        "fix_seq": 7,
        "src_partition": 3,
        "src_offset": 123456,
        "trace_id": "6f1c2a8e-1b7e-4c55-9d1e-2f3a4b5c6d7e",
        "replay_send_ts": 1_761_609_601_000,
    }
    msg.update(overrides)
    return msg


def row_for(value: bytes | None, partition: int = 2, offset: int = 41) -> dict[str, Any]:
    return dict(zip(COLUMNS, to_row(value, partition, offset, KAFKA_TS), strict=True))


def test_valid_track() -> None:
    r = row_for(json.dumps(track()).encode())
    assert r["decode_error"] == ""
    assert (r["mmsi"], r["event_ts"], r["src_partition"], r["src_offset"]) == (219000001, 1_761_609_600_000, 3, 123456)
    assert r["lat"] == 56.4 and r["sog_kn"] == 3.07 and r["fix_seq"] == 7
    assert r["payload"] == b""  # a good record is fully described by its columns
    assert (r["kafka_partition"], r["kafka_offset"], r["kafka_ts"]) == (2, 41, KAFKA_TS)


def test_first_fix_has_null_innovation() -> None:
    r = row_for(json.dumps(track(innovation_m=None, innovation_nis=None, dt_s=None, fix_seq=1)).encode())
    assert r["decode_error"] == ""
    assert r["innovation_m"] is None and r["dt_s"] is None


def test_redelivery_gives_the_same_key() -> None:
    v = json.dumps(track()).encode()
    a, b = row_for(v, 2, 41), row_for(v, 5, 900)
    key = ("mmsi", "event_ts", "src_partition", "src_offset")
    assert [a[k] for k in key] == [b[k] for k in key]


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (None, "empty"),
        (b"\xff", "not_utf8"),
        (b"{", "not_json"),
        (b"[]", "not_object"),
        (json.dumps(track(schema_version=2)).encode(), "schema_version"),
        (json.dumps(track(mmsi="x")).encode(), "bad_key:mmsi"),
        (json.dumps(track(event_ts=None)).encode(), "bad_key:event_ts"),
        (json.dumps(track(src_partition=300)).encode(), "bad_key:src_partition"),
        (json.dumps(track(src_offset=-1)).encode(), "bad_key:src_offset"),
    ],
)
def test_unreadable_identity_gets_a_per_record_key(value: bytes | None, error: str) -> None:
    r = row_for(value, partition=4, offset=77)
    assert r["decode_error"] == error
    # MMSI 0 is never a tracked ship, so this key cannot collide with a real track.
    assert (r["mmsi"], r["event_ts"], r["src_partition"], r["src_offset"]) == (INVALID_MMSI, KAFKA_TS, 4, 77)
    assert r["lat"] is None
    assert r["payload"] == (value or b"")


def test_bad_value_field_keeps_the_identity() -> None:
    v = json.dumps(track(sog_kn="fast")).encode()
    r = row_for(v)
    assert r["decode_error"] == "bad_field:sog_kn"
    assert r["mmsi"] == 219000001 and r["sog_kn"] is None and r["lat"] == 56.4
    assert r["payload"] == v


def test_bool_is_not_a_number() -> None:
    r = row_for(json.dumps(track(fix_seq=True)).encode())
    assert r["decode_error"] == "bad_field:fix_seq"
