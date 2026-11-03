"""Turn one ais.alerts Kafka record into one dvw.alerts row. Pure: no I/O.

The table deduplicates on alert_id (ReplacingMergeTree, sql/clickhouse/0002), so the
tracker can resend an alert after a crash and it is still stored once (ADR 0005).
Never drops a record, like the raw mapper. A record that cannot be used gets
alert_id = "invalid:<partition>:<offset>": unique per Kafka record, so it cannot
collapse into another alert, and a redelivery of the same record still dedups.

decode_error codes: empty, not_utf8, not_json, not_object, schema_version (typed
fields left null), bad_field:<name> (that field null or '', the first one reported).
alert_id must be a canonical lowercase UUID string, as detect.alert_id() writes it: two
spellings of one UUID would be two rows.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from typing import Any

from sinks.clickhouse.convert import BadValue, decode_object, to_float, ts_ms, u32

SCHEMA_VERSION = 1

COLUMNS: tuple[str, ...] = (
    "alert_id",
    "type",
    "mmsi",
    "run_id",
    "ts",
    "lat",
    "lon",
    "pred_lat",
    "pred_lon",
    "details",
    "trace_id",
    "payload",
    "decode_error",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
)
_INDEX = {c: i for i, c in enumerate(COLUMNS)}

TYPED_FIELDS: dict[str, Callable[[Any], object]] = {
    "mmsi": u32,
    "ts": ts_ms,
    "lat": to_float,
    "lon": to_float,
    "pred_lat": to_float,
    "pred_lon": to_float,
}
# Not nullable in the table: '' when missing or wrong.
TEXT_FIELDS = ("type", "run_id", "trace_id")


def invalid_id(partition: int, offset: int) -> str:
    return f"invalid:{partition}:{offset}"


def _alert_id(v: Any) -> str:
    if not isinstance(v, str):
        raise BadValue
    try:
        canonical = str(uuid.UUID(v))
    except ValueError:
        raise BadValue from None
    if canonical != v:
        raise BadValue
    return v


def _details(v: Any) -> str:
    """The rule-specific object as compact JSON text (query with JSONExtract*)."""
    if v is None:
        return ""
    if not isinstance(v, dict):
        raise BadValue
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def to_row(value: bytes | None, partition: int, offset: int, kafka_ts_ms: int) -> list[object]:
    """Return the row's values in COLUMNS order. Never raises for any value."""
    row: list[object] = [None] * len(COLUMNS)
    row[_INDEX["alert_id"]] = invalid_id(partition, offset)
    for f in (*TEXT_FIELDS, "details"):
        row[_INDEX[f]] = ""
    row[_INDEX["payload"]] = value or b""
    row[_INDEX["kafka_partition"]] = partition
    row[_INDEX["kafka_offset"]] = offset
    row[_INDEX["kafka_ts"]] = kafka_ts_ms
    row[_INDEX["decode_error"]] = _fill(row, value)
    return row


def _fill(row: list[object], value: bytes | None) -> str:
    """Fill decoded fields into row; return the decode_error code ('' if none)."""
    obj, error, _ = decode_object(value)
    if obj is None:
        return error
    if obj.get("schema_version") != SCHEMA_VERSION:
        return "schema_version"

    def put(name: str, conv: Callable[[Any], object]) -> None:
        nonlocal error
        try:
            row[_INDEX[name]] = conv(obj.get(name))
        except BadValue:
            error = error or f"bad_field:{name}"

    put("alert_id", _alert_id)
    for f in TEXT_FIELDS:
        put(f, lambda v: "" if v is None else _text(v))
    put("details", _details)
    for f, conv in TYPED_FIELDS.items():
        put(f, conv)
    return error


def _text(v: Any) -> str:
    if isinstance(v, str):
        return v
    raise BadValue
