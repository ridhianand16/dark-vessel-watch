"""Turn one ais.tracks Kafka record into one dvw.tracks row. Pure: no I/O.

The table deduplicates on (mmsi, event_ts, src_partition, src_offset), the fix's
identity (ReplacingMergeTree, sql/clickhouse/0003). The tracker's output is a pure
function of its state and the input record, so a track it resends after a crash is
byte-identical and collapses into the first copy (stream/faust_app/tracker.py).

Never drops a record, like the other mappers. If the identity fields cannot be read
(not JSON, wrong schema_version, a bad mmsi, event_ts, src_partition or src_offset), the
row gets mmsi = 0 and the record's own Kafka coordinates and timestamp as the key.
MMSI 0 is not a ship, so the tracker never emits it and such a row cannot collapse into
a real track; a redelivery of the same record still dedups. The rollups skip any row
with a decode_error.

decode_error codes: empty, not_utf8, not_json, not_object, schema_version, bad_key:<name>
(identity unreadable), bad_field:<name> (that field null, the first one reported).

payload (the original bytes) is kept only when decode_error is set. A good record is
fully described by its columns, and storing every track twice would double the table.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sinks.clickhouse.convert import BadValue, decode_object, int_in, to_float, ts_ms, u8, u32

SCHEMA_VERSION = 1
INVALID_MMSI = 0

COLUMNS: tuple[str, ...] = (
    "mmsi",
    "event_ts",
    "src_partition",
    "src_offset",
    "run_id",
    "lat",
    "lon",
    "vx_ms",
    "vy_ms",
    "sog_kn",
    "cog_deg",
    "pos_sigma_m",
    "meas_lat",
    "meas_lon",
    "innovation_m",
    "innovation_nis",
    "dt_s",
    "fix_seq",
    "trace_id",
    "replay_send_ts",
    "payload",
    "decode_error",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
)
_INDEX = {c: i for i, c in enumerate(COLUMNS)}

u64 = int_in(0, 2**64 - 1)


def _required(conv: Callable[[Any], object]) -> Callable[[Any], object]:
    def req(v: Any) -> object:
        out = conv(v)
        if out is None:
            raise BadValue
        return out

    return req


KEY_FIELDS: dict[str, Callable[[Any], object]] = {
    "mmsi": _required(u32),
    "event_ts": _required(ts_ms),
    "src_partition": _required(u8),
    "src_offset": _required(u64),
}
FLOAT_FIELDS = (
    "lat", "lon", "vx_ms", "vy_ms", "sog_kn", "cog_deg", "pos_sigma_m",
    "meas_lat", "meas_lon", "innovation_m", "innovation_nis", "dt_s",
)  # fmt: skip
TYPED_FIELDS: dict[str, Callable[[Any], object]] = {
    **{f: to_float for f in FLOAT_FIELDS},
    "fix_seq": u32,
    "replay_send_ts": ts_ms,
}
# Not nullable in the table: '' when missing or wrong.
TEXT_FIELDS = ("run_id", "trace_id")


def to_row(value: bytes | None, partition: int, offset: int, kafka_ts_ms: int) -> list[object]:
    """Return the row's values in COLUMNS order. Never raises for any value."""
    row: list[object] = [None] * len(COLUMNS)
    _set_invalid_key(row, partition, offset, kafka_ts_ms)
    for f in TEXT_FIELDS:
        row[_INDEX[f]] = ""
    row[_INDEX["kafka_partition"]] = partition
    row[_INDEX["kafka_offset"]] = offset
    row[_INDEX["kafka_ts"]] = kafka_ts_ms
    error = _fill(row, value)
    row[_INDEX["decode_error"]] = error
    row[_INDEX["payload"]] = (value or b"") if error else b""
    return row


def _set_invalid_key(row: list[object], partition: int, offset: int, kafka_ts_ms: int) -> None:
    row[_INDEX["mmsi"]] = INVALID_MMSI
    row[_INDEX["event_ts"]] = kafka_ts_ms
    row[_INDEX["src_partition"]] = partition
    row[_INDEX["src_offset"]] = offset


def _fill(row: list[object], value: bytes | None) -> str:
    """Fill decoded fields into row; return the decode_error code ('' if none)."""
    obj, error, _ = decode_object(value)
    if obj is None:
        return error
    if obj.get("schema_version") != SCHEMA_VERSION:
        return "schema_version"

    key: dict[str, object] = {}
    for name, conv in KEY_FIELDS.items():
        try:
            key[name] = conv(obj.get(name))
        except BadValue:
            # All or nothing: a partial key could collide with a real track.
            return f"bad_key:{name}"
    for name, v in key.items():
        row[_INDEX[name]] = v

    def put(name: str, conv: Callable[[Any], object]) -> None:
        nonlocal error
        try:
            row[_INDEX[name]] = conv(obj.get(name))
        except BadValue:
            error = error or f"bad_field:{name}"

    for f in TEXT_FIELDS:
        put(f, lambda v: "" if v is None else _text(v))
    for f, conv in TYPED_FIELDS.items():
        put(f, conv)
    return error


def _text(v: Any) -> str:
    if isinstance(v, str):
        return v
    raise BadValue
