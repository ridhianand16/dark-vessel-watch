"""Staging checks against the real PostGIS (make up). Skipped when it is not reachable.

Each test works in its own schema inside a transaction that is rolled back, so nothing
is left behind.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg import sql

from common.postgis import conninfo
from refdata.area import BBox
from refdata.staging import StagingCheckFailed, check_staging

BBOX = BBox(min_lon=2.6909, min_lat=53.5224, max_lon=17.5334, max_lat=59.5686)

SQUARE = "SRID=4326;MULTIPOLYGON(((10 55,11 55,11 56,10 56,10 55)))"
BOWTIE = "SRID=4326;MULTIPOLYGON(((10 55,11 56,11 55,10 56,10 55)))"  # self-intersecting
LINE = "SRID=4326;LINESTRING(9 55,12 57)"
PORT = "SRID=4326;POLYGON((12.5 55.6,12.6 55.6,12.6 55.7,12.5 55.7,12.5 55.6))"


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    try:
        c = psycopg.connect(conninfo(), connect_timeout=2)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostGIS not reachable: {exc}")
    try:
        yield c
    finally:
        c.rollback()
        c.close()


@pytest.fixture
def schema(conn: psycopg.Connection[Any]) -> str:
    name = f"test_staging_{uuid.uuid4().hex[:8]}"
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
    return name


def layer(conn: psycopg.Connection[Any], schema: str, name: str, geom_type: str, wkts: list[str | None]) -> None:
    table = sql.Identifier(schema, name)
    conn.execute(
        sql.SQL("CREATE TABLE {} (id serial PRIMARY KEY, geom geometry({}, 4326))").format(table, sql.SQL(geom_type))
    )
    for wkt in wkts:
        conn.execute(sql.SQL("INSERT INTO {} (geom) VALUES (ST_GeomFromEWKT(%s))").format(table), (wkt,))


def test_valid_layers_pass(conn: psycopg.Connection[Any], schema: str) -> None:
    layer(conn, schema, "eez", "MultiPolygon", [SQUARE])
    layer(conn, schema, "eez_boundaries", "LineString", [LINE])
    reports = check_staging(conn, schema, ["eez", "eez_boundaries"], BBOX)
    assert [(r.layer, r.rows, r.repaired) for r in reports] == [("eez", 1, 0), ("eez_boundaries", 1, 0)]
    assert reports[0].extent == BBox(10, 55, 11, 56)


def test_invalid_geometry_is_repaired_and_counted(
    conn: psycopg.Connection[Any], schema: str, caplog: pytest.LogCaptureFixture
) -> None:
    layer(conn, schema, "wdpa_marine", "MultiPolygon", [SQUARE, BOWTIE])
    with caplog.at_level(logging.WARNING, logger="refdata.staging"):
        (report,) = check_staging(conn, schema, ["wdpa_marine"], BBOX)
    assert report.repaired == 1
    assert "repaired 1 invalid geometries" in caplog.text
    row = conn.execute(
        sql.SQL("SELECT bool_and(ST_IsValid(geom)), min(GeometryType(geom)) FROM {}").format(
            sql.Identifier(schema, "wdpa_marine")
        )
    ).fetchone()
    assert row == (True, "MULTIPOLYGON")


def test_every_problem_is_reported_and_nothing_passes(conn: psycopg.Connection[Any], schema: str) -> None:
    layer(conn, schema, "eez", "MultiPolygon", [SQUARE])
    layer(conn, schema, "eez_boundaries", "LineString", [])
    layer(conn, schema, "wpi_ports", "Polygon", [PORT, None])
    layer(conn, schema, "wdpa_marine", "LineString", ["SRID=4326;LINESTRING(-10 40,12 57)"])
    with pytest.raises(StagingCheckFailed) as exc:
        check_staging(conn, schema, ["eez", "eez_boundaries", "wpi_ports", "wdpa_marine"], BBOX)
    msg = str(exc.value)
    assert "eez_boundaries: no rows" in msg
    assert "wpi_ports: 1 rows without a geometry" in msg
    assert "wdpa_marine: extent" in msg and "not inside the package bbox" in msg
    assert "eez:" not in msg.replace("eez_boundaries", "")


def test_missing_table_fails(conn: psycopg.Connection[Any], schema: str) -> None:
    with pytest.raises(psycopg.errors.UndefinedTable):
        check_staging(conn, schema, ["eez"], BBOX)
