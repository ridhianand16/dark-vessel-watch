"""Turn one CSV line into an ais.raw message (schema_version 1).

No validation happens here (that is M4's job in Flink). Every line becomes a message.
A value that does not parse becomes null, and the original line is always in `raw`,
so the DLQ can later report exactly what arrived.

ais.raw v1 value (JSON, UTF-8):
    schema_version  int     1
    event_ts        int?    epoch milliseconds UTC, from the CSV
    mmsi            int?
    lat, lon, sog, cog      float?   (non-finite values become null)
    heading, imo, length, width  int?
    nav_status, ship_type, name, callsign  str?  (empty string becomes null; text kept as-is)
    raw             str     original CSV line, without the newline
    replay_send_ts  int     epoch milliseconds, wall clock just before produce()
    trace_id        str     uuid4
    run_id          str     the replay run (same as data/runs/<run_id>/), for per-run reconciliation
Key: str(mmsi), or "invalid" when the MMSI does not parse.
"""

from __future__ import annotations

import csv
import json
import math
import time
import uuid
from collections.abc import Callable
from typing import Any

SCHEMA_VERSION = 1
INVALID_KEY = b"invalid"

FLOAT_FIELDS = ("lat", "lon", "sog", "cog")
INT_FIELDS = ("heading", "imo", "length", "width")
STR_FIELDS = ("nav_status", "ship_type", "name", "callsign")


def parse_float(s: str | None) -> float | None:
    if s is None:
        return None
    try:
        f = float(s)
    except ValueError:
        return None
    return f if math.isfinite(f) else None


def parse_int(s: str | None) -> int | None:
    if s is None:
        return None
    try:
        return int(s)
    except ValueError:
        f = parse_float(s)
        return int(f) if f is not None and f.is_integer() else None


def parse_str(s: str | None) -> str | None:
    if s is None:
        return None
    s = s.strip()
    return s or None


def split_line(line: str) -> list[str] | None:
    """Split one CSV line. None if it cannot be parsed at all (e.g. an unclosed quote)."""
    if '"' not in line:
        return line.split(",")  # fast path: DMA rows are never quoted
    try:
        return next(csv.reader([line], strict=True))
    except (csv.Error, StopIteration):
        return None


def build_message(
    line: str | None,
    event_ts_ms: int | None,
    field_index: dict[str, int],
    run_id: str,
    *,
    now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
) -> tuple[bytes, bytes]:
    """Return (key, value) for one line."""
    raw = line if line is not None else ""
    cells = split_line(raw)

    def cell(field: str) -> str | None:
        i = field_index.get(field)
        if cells is None or i is None or i >= len(cells):
            return None
        return cells[i]

    mmsi = parse_int(cell("mmsi"))
    msg: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "event_ts": event_ts_ms, "mmsi": mmsi}
    for f in FLOAT_FIELDS:
        msg[f] = parse_float(cell(f))
    for f in INT_FIELDS:
        msg[f] = parse_int(cell(f))
    for f in STR_FIELDS:
        msg[f] = parse_str(cell(f))
    msg["raw"] = raw
    msg["replay_send_ts"] = now_ms()
    msg["trace_id"] = str(uuid.uuid4())
    msg["run_id"] = run_id
    key = str(mmsi).encode() if mmsi is not None else INVALID_KEY
    value = json.dumps(msg, ensure_ascii=False, separators=(",", ":")).encode()
    return key, value
