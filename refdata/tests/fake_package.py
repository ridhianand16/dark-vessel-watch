"""Write small signed zone packages for tests: minimal GeoPackages made with sqlite3."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import struct
from pathlib import Path
from typing import Any

from refdata.area import BBox
from refdata.package import FORMAT, LAYERS, MANIFEST, SIGNATURE, trusted_comment
from refdata.tests.minisign_keys import SigningKey

Ring = list[tuple[float, float]]


def _points(points: Ring) -> bytes:
    return struct.pack("<I", len(points)) + b"".join(struct.pack("<dd", x, y) for x, y in points)


def multipolygon(*rings: Ring) -> bytes:
    """WKB MultiPolygon with one single-ring polygon per ring."""
    parts = [b"\x01" + struct.pack("<II", 3, 1) + _points(r) for r in rings]
    return b"\x01" + struct.pack("<II", 6, len(parts)) + b"".join(parts)


def multilinestring(*lines: Ring) -> bytes:
    parts = [b"\x01" + struct.pack("<I", 2) + _points(line) for line in lines]
    return b"\x01" + struct.pack("<II", 5, len(parts)) + b"".join(parts)


def gpkg_blob(wkb: bytes, srs_id: int = 4326) -> bytes:
    return b"GP\x00\x01" + struct.pack("<i", srs_id) + wkb  # little endian, no envelope


def square(lon: float, lat: float, size: float = 0.1) -> Ring:
    return [(lon, lat), (lon + size, lat), (lon + size, lat + size), (lon, lat + size), (lon, lat)]


def write_layer(path: Path, layer: str, columns: tuple[str, ...], rows: list[tuple[Any, ...]], geometry: str) -> None:
    db = sqlite3.connect(path)
    with db:
        db.execute(
            "CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, geometry_type_name TEXT, srs_id INT)"
        )
        db.execute("INSERT INTO gpkg_geometry_columns VALUES (?, 'geom', ?, 4326)", (layer, geometry))
        cols = ", ".join(f'"{c}"' for c in columns)
        db.execute(f'CREATE TABLE "{layer}" (fid INTEGER PRIMARY KEY, {cols}, geom BLOB)')
        marks = ", ".join("?" * (len(columns) + 1))
        db.executemany(f'INSERT INTO "{layer}" ({cols}, geom) VALUES ({marks})', rows)
    db.close()


def default_rows() -> dict[str, list[tuple[Any, ...]]]:
    return {
        "eez": [("eez:5674", "eez", "Danish Exclusive Economic Zone", "eez:v12", gpkg_blob(multipolygon(square(10, 56, 1))))],
        "mpa": [
            ("wdpa:1", "mpa", "Reef (Natura 2000)", "wdpa_marine:Nov2026", gpkg_blob(multipolygon(square(11, 56)))),
            ("wdpa:2", "mpa", "Bay", "wdpa_marine:Nov2026", gpkg_blob(multipolygon(square(11.5, 56.5)))),
        ],
        "ports": [("wpi:29230", "port", "KOBENHAVN, DK", "wpi:2019", gpkg_blob(multipolygon(square(12.55, 55.65))))],
        "eez_boundaries": [(1120, gpkg_blob(multilinestring([(11.0, 56.0), (12.0, 57.0)])))],
    }


def make_package(
    root: Path,
    key: SigningKey,
    bbox: BBox,
    version: str = "zones-test00000001",
    rows: dict[str, list[tuple[Any, ...]]] | None = None,
) -> Path:
    rows = rows if rows is not None else default_rows()
    pkg = root / version
    pkg.mkdir(parents=True)
    files: dict[str, Any] = {}
    for layer in LAYERS:
        path = pkg / f"{layer.name}.gpkg"
        geometry = "MULTILINESTRING" if layer.table == "eez_boundaries" else "MULTIPOLYGON"
        write_layer(path, layer.name, layer.columns, rows[layer.name], geometry)
        data = path.read_bytes()
        files[path.name] = {
            "layer": layer.name,
            "table": layer.table,
            "features": len(rows[layer.name]),
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    manifest = {"format": FORMAT, "package_version": version, "area": {"bbox": bbox.as_dict()}, "files": files}
    body = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    (pkg / MANIFEST).write_bytes(body)
    (pkg / SIGNATURE).write_text(key.sign(body, trusted_comment(version)), encoding="utf-8")
    return pkg


def write_area(path: Path, bbox: BBox) -> Path:
    path.write_text(json.dumps({"bbox": bbox.as_dict()}), encoding="utf-8")  # JSON is valid YAML
    return path
