from __future__ import annotations

import json
from typing import Any

import pytest

from sinks.clickhouse.alerts import COLUMNS, to_row

KAFKA_TS = 1_761_609_601_500
ALERT_ID = "0b2e6c1a-4f3d-5a7e-9c1b-2d3e4f5a6b7c"


def alert(**overrides: Any) -> dict[str, Any]:
    """An ais.alerts v1 value as stream/faust_app/detect.encode_alert() writes it."""
    msg: dict[str, Any] = {
        "schema_version": 1,
        "alert_id": ALERT_ID,
        "type": "DARK_GAP",
        "mmsi": 999000001,
        "run_id": "20261103T120000Z-abc123",
        "ts": 1_761_611_400_000,
        "lat": 56.4,
        "lon": 11.8,
        "pred_lat": 56.45,
        "pred_lon": 11.9,
        "details": {"last_seen_ts": 1_761_609_600_000, "gap_s": 1800.0},
        "trace_id": "6f1c2a8e-1b7e-4c55-9d1e-2f3a4b5c6d7e",
    }
    msg.update(overrides)
    return msg


def row_for(value: bytes | None, partition: int = 2, offset: int = 41) -> dict[str, Any]:
    return dict(zip(COLUMNS, to_row(value, partition, offset, KAFKA_TS), strict=True))


def test_valid_alert() -> None:
    r = row_for(json.dumps(alert()).encode())
    assert r["decode_error"] == ""
    assert (r["alert_id"], r["type"], r["mmsi"], r["ts"]) == (ALERT_ID, "DARK_GAP", 999000001, 1_761_611_400_000)
    assert (r["pred_lat"], r["pred_lon"]) == (56.45, 11.9)
    assert json.loads(r["details"]) == {"last_seen_ts": 1_761_609_600_000, "gap_s": 1800.0}
    assert (r["kafka_partition"], r["kafka_offset"], r["kafka_ts"]) == (2, 41, KAFKA_TS)


def test_resent_alert_maps_to_the_same_identity() -> None:
    """The tracker resends an alert after a crash: another Kafka record, same alert_id."""
    value = json.dumps(alert()).encode()
    first, again = row_for(value, 2, 41), row_for(value, 2, 97)
    assert first["alert_id"] == again["alert_id"] == ALERT_ID
    assert {k: v for k, v in first.items() if not k.startswith("kafka_")} == {
        k: v for k, v in again.items() if not k.startswith("kafka_")
    }


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (None, "empty"),
        (b"\xff\xfe", "not_utf8"),
        (b"{trunc", "not_json"),
        (b"[1]", "not_object"),
        (json.dumps(alert(schema_version=2)).encode(), "schema_version"),
        (json.dumps(alert(alert_id=None)).encode(), "bad_field:alert_id"),
        (json.dumps(alert(alert_id="not-a-uuid")).encode(), "bad_field:alert_id"),
        (json.dumps(alert(alert_id=ALERT_ID.upper())).encode(), "bad_field:alert_id"),
    ],
)
def test_unusable_alert_is_kept_under_a_per_record_id(value: bytes | None, error: str) -> None:
    r = row_for(value)
    assert r["decode_error"] == error
    assert r["alert_id"] == "invalid:2:41"
    assert r["payload"] == (value or b"")


def test_bad_field_keeps_the_rest() -> None:
    r = row_for(json.dumps(alert(mmsi="219", details=[1], lat="x")).encode())
    assert r["alert_id"] == ALERT_ID  # still dedups
    assert r["decode_error"] == "bad_field:details"  # first bad field in check order
    assert (r["mmsi"], r["lat"], r["details"]) == (None, None, "")
    assert r["type"] == "DARK_GAP"


def test_missing_trace_id_is_empty_text() -> None:
    r = row_for(json.dumps(alert(trace_id=None)).encode())
    assert (r["trace_id"], r["decode_error"]) == ("", "")
