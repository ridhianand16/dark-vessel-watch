"""Read features from a GeoPackage with sqlite3, so loading a package needs no GDAL.

A GeoPackage is an SQLite file. Each geometry is a blob: a small header followed by
standard WKB, which PostGIS reads with ST_GeomFromWKB.

    "GP" | version (1 byte) | flags (1 byte) | srs_id (int32) | envelope | WKB

flags bit 0 is the byte order of srs_id and the envelope, bits 1-3 the envelope type
(0 none, 1 xy, 2 xyz, 3 xym, 4 xyzm), bit 4 marks an empty geometry.
"""

from __future__ import annotations

import sqlite3
import struct
from collections.abc import Iterator
from pathlib import Path
from typing import Any

ENVELOPE_BYTES = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}


class GpkgError(Exception):
    pass


def split_blob(blob: bytes) -> tuple[int, bytes | None]:
    """(srs_id, WKB) of a GeoPackage geometry blob. WKB is None for an empty geometry."""
    if len(blob) < 8 or blob[:2] != b"GP":
        raise GpkgError("not a GeoPackage geometry blob")
    flags = blob[3]
    (srs_id,) = struct.unpack("<i" if flags & 1 else ">i", blob[4:8])
    envelope = (flags >> 1) & 0b111
    if envelope not in ENVELOPE_BYTES:
        raise GpkgError(f"invalid envelope type {envelope}")
    if flags & 0b10000:
        return srs_id, None
    return srs_id, blob[8 + ENVELOPE_BYTES[envelope] :]


def _connect(path: Path) -> sqlite3.Connection:
    # Read-only URI: verifying hashes first is pointless if opening could change the file.
    return sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)


def geometry_column(path: Path, layer: str) -> tuple[str, str, int]:
    """(column, geometry type, srs_id) of layer from gpkg_geometry_columns."""
    with _connect(path) as db:
        row = db.execute(
            "SELECT column_name, geometry_type_name, srs_id FROM gpkg_geometry_columns WHERE table_name = ?", (layer,)
        ).fetchone()
    if row is None:
        raise GpkgError(f"{path.name}: no geometry layer {layer!r}")
    return str(row[0]), str(row[1]), int(row[2])


def count(path: Path, layer: str) -> int:
    with _connect(path) as db:
        (n,) = db.execute(f'SELECT count(*) FROM "{layer}"').fetchone()
    return int(n)


def read_features(path: Path, layer: str, columns: list[str]) -> Iterator[tuple[Any, ...]]:
    """Rows of columns plus (srs_id, WKB) of the layer's geometry, in fid order."""
    geom, _, _ = geometry_column(path, layer)
    names = ", ".join(f'"{c}"' for c in [*columns, geom])
    db = _connect(path)
    try:
        for row in db.execute(f'SELECT {names} FROM "{layer}" ORDER BY fid'):
            blob = row[-1]
            srs_id, wkb = split_blob(blob) if blob is not None else (0, None)
            yield (*row[:-1], srs_id, wkb)
    finally:
        db.close()
