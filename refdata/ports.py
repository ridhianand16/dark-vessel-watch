"""Port zones: World Port Index points turned into circles of a fixed radius on the ground.

The radius is metres on the WGS84 ellipsoid, not degrees (ADR 0010). Each vertex is
offset from the port along a bearing using the ellipsoid's radii of curvature at the
mid-latitude between port and vertex. For radii of a few nautical miles the error is
far below a metre; refdata/tests/test_ports.py measures it with PostGIS geography.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from refdata.area import BBox

METRES_PER_NM = 1852.0
SEGMENTS = 64  # chord error at 2 nm: r(1 - cos(pi/64)) = 4.5 m inwards
WGS84_A = 6378137.0
WGS84_F = 1 / 298.257223563
WGS84_E2 = WGS84_F * (2 - WGS84_F)


def _radii(lat_rad: float) -> tuple[float, float]:
    """Meridional (M) and prime-vertical (N) radii of curvature at a latitude."""
    w = 1 - WGS84_E2 * math.sin(lat_rad) ** 2
    return WGS84_A * (1 - WGS84_E2) / w**1.5, WGS84_A / math.sqrt(w)


def circle(lon: float, lat: float, radius_m: float, segments: int = SEGMENTS) -> list[list[float]]:
    """Closed ring of (lon, lat) around a point, counter-clockwise (GeoJSON exterior order)."""
    phi = math.radians(lat)
    ring: list[list[float]] = []
    for k in range(segments):
        bearing = -2 * math.pi * k / segments  # decreasing bearing = counter-clockwise
        m, _ = _radii(phi)
        dphi = radius_m * math.cos(bearing) / m
        _, n = _radii(phi + dphi / 2)
        dlam = radius_m * math.sin(bearing) / (n * math.cos(phi + dphi / 2))
        ring.append([lon + math.degrees(dlam), lat + math.degrees(dphi)])
    ring.append(ring[0])
    return ring


def _near(lon: float, lat: float, bbox: BBox, radius_m: float) -> bool:
    """Could a circle around (lon, lat) reach into bbox? Generous: ogr2ogr clips exactly."""
    pad_lat = math.degrees(radius_m / WGS84_A) * 1.01
    pad_lon = pad_lat / max(math.cos(math.radians(min(abs(lat) + pad_lat, 89.9))), 1e-6)
    return (
        bbox.min_lon - pad_lon <= lon <= bbox.max_lon + pad_lon
        and bbox.min_lat - pad_lat <= lat <= bbox.max_lat + pad_lat
    )


def port_circles(wpi: dict[str, Any], bbox: BBox, radius_nm: float) -> Iterator[dict[str, Any]]:
    """GeoJSON polygon features for WPI ports whose circle can reach bbox."""
    radius_m = radius_nm * METRES_PER_NM
    for feature in wpi["features"]:
        geometry = feature.get("geometry") or {}
        if geometry.get("type") != "Point":
            continue
        lon, lat = (float(c) for c in geometry["coordinates"][:2])
        if not _near(lon, lat, bbox, radius_m):
            continue
        props = feature["properties"]
        yield {
            "type": "Feature",
            "properties": {
                "INDEX_NO": int(props["INDEX_NO"]),
                "PORT_NAME": str(props["PORT_NAME"]).strip(),
                "COUNTRY": str(props["COUNTRY"]).strip(),
            },
            "geometry": {"type": "Polygon", "coordinates": [circle(lon, lat, radius_m)]},
        }


def write_port_circles(wpi_geojson: Path, out: Path, bbox: BBox, radius_nm: float) -> int:
    wpi: dict[str, Any] = json.loads(wpi_geojson.read_text(encoding="utf-8"))
    features = list(port_circles(wpi, bbox, radius_nm))
    out.write_text(json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8")
    return len(features)
