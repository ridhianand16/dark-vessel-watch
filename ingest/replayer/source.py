"""Read an AIS CSV in event-time order.

The file is scanned by polars as *one string column per line* (the separator is a
control byte that does not occur in the data, and quoting is off). That keeps the
exact original line for the `raw` field, and a malformed row cannot make the scan
fail or be dropped: field parsing happens later, per row, in record.py.

Only the timestamp is parsed in polars, to build the sort key. Sorting is by
(event_ts, line number), so the order is deterministic for rows that share a second.
Rows whose timestamp does not parse sort last and are sent without pacing.
"""

from __future__ import annotations

import csv
import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl

# Normalised header name -> message field. Normalisation: strip, drop a leading "#",
# lowercase. DMA's first column is literally "# Timestamp".
COLUMN_FIELDS: dict[str, str] = {
    "timestamp": "event_ts",
    "mmsi": "mmsi",
    "latitude": "lat",
    "longitude": "lon",
    "sog": "sog",
    "cog": "cog",
    "heading": "heading",
    "navigational status": "nav_status",
    "ship type": "ship_type",
    "name": "name",
    "imo": "imo",
    "callsign": "callsign",
    "length": "length",
    "width": "width",
}
REQUIRED_FIELDS = ("event_ts", "mmsi")

# Tried in order against the first data row. DMA uses day-first; there is
# deliberately no month-first candidate, so 01/02 cannot be silently misread.
TIMESTAMP_FORMATS = ("%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")

LINE_SEPARATOR = "\x1f"  # ASCII unit separator; never appears in AIS CSVs


class SourceError(Exception):
    """The CSV header or first row is not something we can replay."""


@dataclass(frozen=True)
class CsvLayout:
    columns: tuple[str, ...]  # header as it appears in the file
    field_index: dict[str, int]  # message field -> column position
    timestamp_format: str


def normalise_header(name: str) -> str:
    return name.strip().lstrip("#").strip().lower()


def _detect_timestamp_format(value: str) -> str:
    for fmt in TIMESTAMP_FORMATS:
        try:
            dt.datetime.strptime(value.strip(), fmt)
        except ValueError:
            continue
        return fmt
    raise SourceError(f"first row timestamp {value!r} matches none of {TIMESTAMP_FORMATS}")


def read_layout(path: Path) -> CsvLayout:
    """Read the real header and first data row; map columns and detect the timestamp format."""
    with path.open(encoding="utf-8", errors="replace", newline="") as f:
        header_line = f.readline()
        first_line = f.readline()
    columns = tuple(next(csv.reader([header_line])))
    field_index: dict[str, int] = {}
    for i, col in enumerate(columns):
        field = COLUMN_FIELDS.get(normalise_header(col))
        if field is not None and field not in field_index:
            field_index[field] = i
    missing = [f for f in REQUIRED_FIELDS if f not in field_index]
    if missing:
        raise SourceError(f"{path}: header has no column for {missing}; header is {columns}")
    if not first_line:
        raise SourceError(f"{path}: no data rows")
    first_row = next(csv.reader([first_line]))
    ts_idx = field_index["event_ts"]
    if ts_idx >= len(first_row):
        raise SourceError(f"{path}: first data row has no timestamp column")
    return CsvLayout(columns, field_index, _detect_timestamp_format(first_row[ts_idx]))


def sorted_lines(
    path: Path, layout: CsvLayout, limit: int | None, chunk_size: int = 50_000
) -> Iterator[tuple[list[str | None], list[int | None]]]:
    """Yield (lines, event_ts_ms) batches in event-time order.

    With a limit, polars turns sort + head into a top-k, so a smoke test does not
    sort the whole file. Measured on the 3.2 GB / 18.9M-row DMA day: full sort peaks
    at about 1 GB RSS.
    """
    ts_idx = layout.field_index["event_ts"]
    ts_text = (
        pl.col("line")
        .str.splitn(",", ts_idx + 2)
        .struct.field(f"field_{ts_idx}")
        .str.strip_chars()
        .str.strip_chars('"')
    )
    lf = (
        pl.scan_csv(
            path,
            has_header=False,
            skip_rows=1,
            separator=LINE_SEPARATOR,
            quote_char=None,
            schema={"line": pl.String},
            encoding="utf8-lossy",
        )
        .with_row_index("line_no")
        .with_columns(
            # DMA timestamps are UTC; epoch() on a naive datetime treats it as UTC.
            event_ts=ts_text.str.strptime(pl.Datetime("ms"), layout.timestamp_format, strict=False)
            .dt.epoch("ms")
        )
        .sort(["event_ts", "line_no"], nulls_last=True)
        .select("line", "event_ts")
    )
    if limit is not None:
        lf = lf.head(limit)
    for batch in lf.collect_batches(chunk_size=chunk_size):
        yield batch["line"].to_list(), batch["event_ts"].to_list()
