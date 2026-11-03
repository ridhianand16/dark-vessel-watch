"""Value converters shared by the row mappers. Pure: no I/O.

Each converter returns the value as its column wants it, None for a missing value, or
raises BadValue when the value has the wrong type or does not fit the column. Mappers
catch BadValue per field and record it as decode_error = bad_field:<name>.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from typing import Any

# Epoch-ms timestamps outside 2000..2100 are treated as bad values. DateTime64 would
# accept a wider range, but the raw table is partitioned by day, and one insert may
# touch at most 100 partitions (max_partitions_per_insert_block). Junk dates spread
# across decades could make a whole batch fail to insert, every time it is retried.
TS_MIN_MS = 946_684_800_000  # 2000-01-01
TS_MAX_MS = 4_102_444_800_000  # 2100-01-01


class BadValue(ValueError):
    pass


def int_in(lo: int, hi: int) -> Callable[[Any], int | None]:
    def conv(v: Any) -> int | None:
        if v is None:
            return None
        if isinstance(v, bool) or not isinstance(v, int) or not lo <= v <= hi:
            raise BadValue
        return v

    return conv


def to_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v):
        raise BadValue
    return float(v)


def to_str(v: Any) -> str | None:
    if v is None or isinstance(v, str):
        return v
    raise BadValue


ts_ms = int_in(TS_MIN_MS, TS_MAX_MS - 1)
u8, u16, u32 = int_in(0, 2**8 - 1), int_in(0, 2**16 - 1), int_in(0, 2**32 - 1)


def decode_object(value: bytes | None) -> tuple[dict[str, Any] | None, str, str]:
    """(object, error, text) for a Kafka value that should be a JSON object.

    error is '' on success, else one of empty, not_utf8, not_json, not_object. text is
    the value as far as it could be decoded (lossy for not_utf8), for salvaging.
    """
    if not value:
        return None, "empty", ""
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError:
        return None, "not_utf8", value.decode("utf-8", errors="replace")
    try:
        obj = json.loads(text)
    except (ValueError, RecursionError):  # RecursionError: deeply nested input
        return None, "not_json", text
    if not isinstance(obj, dict):
        return None, "not_object", text
    return obj, "", text
