from __future__ import annotations

import json
from typing import Any

import pytest

from sinks.clickhouse.raw import COLUMNS, to_row

KAFKA_TS = 1_761_609_601_500


def message(**overrides: Any) -> dict[str, Any]:
    """An ais.raw v1 value as the replayer produces it."""
    msg: dict[str, Any] = {
        "schema_version": 1,
        "event_ts": 1_761_609_601_000,
        "mmsi": 219026147,
        "lat": 56.610395,
        "lon": 10.30553,
        "sog": 0.0,
        "cog": 315.6,
        "heading": 314,
        "nav_status": "Under way using engine",
        "ship_type": "Cargo",
        "name": "NORDIC STAR",
        "imo": None,
        "callsign": "OXYZ2",
        "length": 89,
        "width": 12,
        "raw": "28/10/2026 00:00:01,Class A,219026147,...",
        "replay_send_ts": 1_762_090_021_000,
        "trace_id": "6f1c2a8e-1b7e-4c55-9d1e-2f3a4b5c6d7e",
        "run_id": "20261102T132701Z-6cdc0c",
    }
    msg.update(overrides)
    return msg


def encode(msg: dict[str, Any]) -> bytes:
    return json.dumps(msg).encode()


def as_dict(value: bytes | None, partition: int = 3, offset: int = 42) -> dict[str, object]:
    row = to_row(value, partition, offset, KAFKA_TS)
    assert len(row) == len(COLUMNS)
    return dict(zip(COLUMNS, row, strict=True))


def test_valid_message_maps_every_field() -> None:
    value = encode(message())
    r = as_dict(value)
    assert r["decode_error"] == ""
    assert r["run_id"] == "20261102T132701Z-6cdc0c"
    assert r["event_ts"] == 1_761_609_601_000  # epoch ms, inserted as DateTime64(3) ticks
    assert (r["mmsi"], r["lat"], r["lon"], r["heading"], r["imo"]) == (219026147, 56.610395, 10.30553, 314, None)
    assert (r["name"], r["length"], r["width"]) == ("NORDIC STAR", 89, 12)
    assert r["payload"] == value
    assert (r["kafka_partition"], r["kafka_offset"], r["kafka_ts"]) == (3, 42, KAFKA_TS)


def test_mapping_is_deterministic_for_redelivery() -> None:
    # Duplicate deliveries must produce identical rows, or the partition key could differ.
    value = encode(message())
    assert to_row(value, 1, 7, KAFKA_TS) == to_row(value, 1, 7, KAFKA_TS)


@pytest.mark.parametrize(
    ("value", "code"),
    [
        (None, "empty"),
        (b"", "empty"),
        (b"\xff\xfe not utf-8", "not_utf8"),
        (b"hello, this is not json", "not_json"),
        (b"[" * 100_000, "not_json"),  # nesting deep enough to hit RecursionError
        (b"[1, 2, 3]", "not_object"),
        (encode(message(schema_version=2)), "schema_version"),
    ],
)
def test_undecodable_values_still_become_rows(value: bytes | None, code: str) -> None:
    r = as_dict(value)
    assert r["decode_error"] == code
    assert r["payload"] == (value or b"")
    assert (r["kafka_partition"], r["kafka_offset"], r["kafka_ts"]) == (3, 42, KAFKA_TS)
    assert r["mmsi"] is None and r["event_ts"] is None


def test_run_id_is_salvaged_from_truncated_json() -> None:
    value = encode(message())[:-1]  # lost the closing brace; run_id (last field) intact
    r = as_dict(value)
    assert r["decode_error"] == "not_json"
    assert r["run_id"] == "20261102T132701Z-6cdc0c"


def test_schema_version_mismatch_keeps_envelope() -> None:
    r = as_dict(encode(message(schema_version=2)))
    assert r["run_id"] == "20261102T132701Z-6cdc0c"
    assert r["trace_id"] != ""


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("mmsi", -1),
        ("mmsi", 2**32),
        ("mmsi", "219026147"),
        ("width", 256),
        ("heading", True),
        ("lat", "56.6"),
        ("event_ts", 1e12),
        ("event_ts", 32_503_680_000_000),  # year 3000: outside the partition-safe range
        ("name", 123),
    ],
)
def test_bad_field_is_nulled_and_others_kept(field: str, bad: object) -> None:
    r = as_dict(encode(message(**{field: bad})))
    assert r["decode_error"] == f"bad_field:{field}"
    assert r[field] is None
    assert r["callsign"] == "OXYZ2"
    assert r["run_id"] == "20261102T132701Z-6cdc0c"


def test_first_bad_field_is_reported() -> None:
    r = as_dict(encode(message(mmsi=-5, width=999)))
    assert r["decode_error"] == "bad_field:mmsi"
    assert r["mmsi"] is None and r["width"] is None


def test_missing_optional_fields_are_null_not_errors() -> None:
    r = as_dict(encode({"schema_version": 1, "run_id": "r1"}))
    assert r["decode_error"] == ""
    assert r["mmsi"] is None
    assert r["raw"] == "" and r["trace_id"] == ""
