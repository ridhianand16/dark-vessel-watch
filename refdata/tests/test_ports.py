from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from common.postgis import conninfo
from refdata.area import BBox
from refdata.ports import METRES_PER_NM, SEGMENTS, circle, port_circles

BBOX = BBox(2.6909, 53.5224, 17.5334, 59.5686)


def test_ring_is_closed_and_counter_clockwise() -> None:
    ring = circle(12.6, 55.7, 3704.0)
    assert len(ring) == SEGMENTS + 1 and ring[0] == ring[-1]
    signed_area = sum(x0 * y1 - x1 * y0 for (x0, y0), (x1, y1) in zip(ring, ring[1:]))
    assert signed_area > 0


def test_only_ports_that_can_reach_the_area_are_kept() -> None:
    def port(index: int, lon: float, lat: float) -> dict[str, Any]:
        return {
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {"INDEX_NO": index, "PORT_NAME": f"P{index} ", "COUNTRY": "DK"},
        }

    wpi = {"features": [port(1, 12.6, 55.7), port(2, 2.66, 55.0), port(3, 2.5, 55.0), port(4, 146.8, 43.9)]}
    kept = list(port_circles(wpi, BBOX, 2.0))
    # Port 2 is 0.03 deg (about 2 km) west of the box, inside 2 nm; port 3 is 12 km out.
    assert [f["properties"]["INDEX_NO"] for f in kept] == [1, 2]
    assert kept[0]["properties"]["PORT_NAME"] == "P1"


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    try:
        c = psycopg.connect(conninfo(), connect_timeout=2)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostGIS not reachable: {exc}")
    with c:
        yield c


@pytest.mark.parametrize("lon, lat", [(12.6, 55.7), (10.0, 59.5), (3.0, 53.6), (0.0, 0.0)])
def test_every_vertex_is_the_radius_away_on_the_ellipsoid(conn: psycopg.Connection[Any], lon: float, lat: float) -> None:
    """PostGIS geography measures on the WGS84 spheroid: the reference for "2 nm on the ground"."""
    radius = 2 * METRES_PER_NM
    wkt = "LINESTRING(" + ",".join(f"{x!r} {y!r}" for x, y in circle(lon, lat, radius)) + ")"
    row = conn.execute(
        "SELECT min(d), max(d) FROM (SELECT ST_Distance(ST_SetSRID(ST_Point(%s, %s), 4326)::geography,"
        " (dp).geom::geography) AS d FROM ST_DumpPoints(ST_GeomFromText(%s, 4326)) dp) s",
        (lon, lat, wkt),
    ).fetchone()
    assert row is not None
    assert abs(row[0] - radius) < 0.5 and abs(row[1] - radius) < 0.5
