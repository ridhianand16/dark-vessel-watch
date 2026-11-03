"""Turn one ais.raw Kafka record into one dvw.ais_raw row. Pure: no I/O.

Every record becomes a row. When the value cannot be used, the row still carries the
Kafka coordinates, the original bytes in `payload`, the run_id if it can be found, and
a short reason code in `decode_error`:

    empty              the record has no value
    not_utf8           value is not UTF-8
    not_json           value is not JSON
    not_object         JSON, but not an object
    schema_version     schema_version is not 1; typed fields are left null
    bad_field:<name>   a field has the wrong type or does not fit its column; that field
                       is null, the others are kept (the first bad field is reported)

This is about storing what arrived, not judging it: an out-of-range latitude is stored
as-is. Validation belongs to the Flink job.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from sinks.clickhouse.convert import BadValue, decode_object, to_float, to_str, ts_ms, u8, u16, u32

SCHEMA_VERSION = 1

COLUMNS: tuple[str, ...] = (
    "run_id",
    "event_ts",
    "replay_send_ts",
    "mmsi",
    "lat",
    "lon",
    "sog",
    "cog",
    "heading",
    "nav_status",
    "ship_type",
    "name",
    "imo",
    "callsign",
    "length",
    "width",
    "raw",
    "payload",
    "decode_error",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
    "trace_id",
)
_INDEX = {c: i for i, c in enumerate(COLUMNS)}

_RUN_ID_RE = re.compile(r'"run_id"\s*:\s*"([A-Za-z0-9_.:-]{1,64})"')

# Payload field -> converter. Insertion order is the order fields are checked in.
TYPED_FIELDS: dict[str, Callable[[Any], object]] = {
    "event_ts": ts_ms,
    "replay_send_ts": ts_ms,
    "mmsi": u32,
    "lat": to_float,
    "lon": to_float,
    "sog": to_float,
    "cog": to_float,
    "heading": u16,
    "nav_status": to_str,
    "ship_type": to_str,
    "name": to_str,
    "imo": u32,
    "callsign": to_str,
    "length": u16,
    "width": u8,
}
# Envelope fields: not nullable in the table, '' when missing or wrong.
TEXT_FIELDS = ("run_id", "raw", "trace_id")


def to_row(value: bytes | None, partition: int, offset: int, kafka_ts_ms: int) -> list[object]:
    """Return the row's values in COLUMNS order. Never raises for any value."""
    row: list[object] = [None] * len(COLUMNS)
    for f in TEXT_FIELDS:
        row[_INDEX[f]] = ""
    row[_INDEX["payload"]] = value or b""  # bytes: stored exactly as received
    row[_INDEX["kafka_partition"]] = partition
    row[_INDEX["kafka_offset"]] = offset
    row[_INDEX["kafka_ts"]] = kafka_ts_ms
    row[_INDEX["decode_error"]] = _fill(row, value)
    return row


def _fill(row: list[object], value: bytes | None) -> str:
    """Fill decoded fields into row; return the decode_error code ('' if none)."""
    obj, error, text = decode_object(value)
    if obj is None:
        if error in ("not_utf8", "not_json"):
            _salvage_run_id(row, text)
        return error

    for f in TEXT_FIELDS:
        v = obj.get(f)
        if isinstance(v, str):
            row[_INDEX[f]] = v
        elif v is not None and not error:
            error = f"bad_field:{f}"
    if obj.get("schema_version") != SCHEMA_VERSION:
        return "schema_version"
    for f, conv in TYPED_FIELDS.items():
        try:
            row[_INDEX[f]] = conv(obj.get(f))
        except BadValue:
            error = error or f"bad_field:{f}"
    return error


def _salvage_run_id(row: list[object], text: str) -> None:
    """Best effort for truncated or corrupted JSON, so reconciliation can attribute it."""
    m = _RUN_ID_RE.search(text)
    if m:
        row[_INDEX["run_id"]] = m.group(1)
