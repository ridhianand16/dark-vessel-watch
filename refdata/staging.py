"""Checks a zone package must pass in the staging schema before it is swapped live.

The loader puts every layer of a verified package into a staging schema, then calls
check_staging() inside the same transaction as the swap. Any failure raises
StagingCheckFailed, the transaction rolls back, and the live zones stay as they were.

Per layer:
    * at least one row, and no row without a geometry;
    * every geometry in the layer's SRID: EPSG:4326, or EPSG:25832 for the metric
      copies (ADR 0013);
    * every geometry valid. Invalid ones are repaired with ST_MakeValid (keeping only
      parts of the original dimension, so a polygon stays a polygon) and the count is
      logged. A geometry still invalid or empty after repair fails the check;
    * the layer's extent inside the package bbox from refdata/area.yaml. Layers are
      clipped to it when the package is built, so anything outside means the package
      was built for a different area. A metric layer's extent is taken after
      transforming back to EPSG:4326, so one bbox in degrees serves both.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg import sql

from refdata.area import BBox

log = logging.getLogger("refdata.staging")

SRID = 4326
EXTENT_TOLERANCE_DEG = 1e-9  # float noise from clipping, far below a millimetre
# A reprojected layer round-trips through PROJ before the extent check; allow ~1 cm.
REPROJECTED_TOLERANCE_DEG = 1e-7


class StagingCheckFailed(Exception):
    """At least one staging layer is unfit to go live. The message lists every problem."""


@dataclass(frozen=True)
class LayerReport:
    layer: str
    rows: int
    repaired: int
    extent: BBox


def _inside(extent: BBox, bbox: BBox, t: float) -> bool:
    return (
        extent.min_lon >= bbox.min_lon - t
        and extent.min_lat >= bbox.min_lat - t
        and extent.max_lon <= bbox.max_lon + t
        and extent.max_lat <= bbox.max_lat + t
    )


def _one(conn: psycopg.Connection[Any], query: sql.Composed) -> tuple[Any, ...]:
    row = conn.execute(query).fetchone()
    assert row is not None
    return tuple(row)


def check_layer(
    conn: psycopg.Connection[Any], schema: str, layer: str, bbox: BBox, geom: str = "geom", srid: int = SRID
) -> tuple[LayerReport | None, list[str]]:
    table = sql.Identifier(schema, layer)
    g = sql.Identifier(geom)
    problems: list[str] = []

    rows, null_geoms, bad_srid = _one(
        conn,
        sql.SQL(
            "SELECT count(*), count(*) FILTER (WHERE {g} IS NULL),"
            " count(*) FILTER (WHERE ST_SRID({g}) <> {srid}) FROM {t}"
        ).format(g=g, t=table, srid=sql.Literal(srid)),
    )
    if rows == 0:
        return None, [f"{layer}: no rows"]
    if null_geoms:
        problems.append(f"{layer}: {null_geoms} rows without a geometry")
    if bad_srid:
        problems.append(f"{layer}: {bad_srid} geometries not in EPSG:{srid}")

    # ST_MakeValid can return a GeometryCollection mixing dimensions (a bow-tie polygon
    # gains a stray line). ST_CollectionExtract keeps the original dimension only, and
    # ST_Multi keeps multi types multi so a typed column still accepts the result.
    repaired = conn.execute(
        sql.SQL(
            "UPDATE {t} SET {g} = CASE WHEN GeometryType({g}) LIKE 'MULTI%'"
            " THEN ST_Multi(ST_CollectionExtract(ST_MakeValid({g}), ST_Dimension({g}) + 1))"
            " ELSE ST_CollectionExtract(ST_MakeValid({g}), ST_Dimension({g}) + 1) END"
            " WHERE {g} IS NOT NULL AND NOT ST_IsValid({g})"
        ).format(g=g, t=table)
    ).rowcount
    if repaired:
        log.warning("%s.%s: repaired %d invalid geometries with ST_MakeValid", schema, layer, repaired)

    (still_bad,) = _one(
        conn,
        sql.SQL("SELECT count(*) FROM {t} WHERE {g} IS NOT NULL AND (NOT ST_IsValid({g}) OR ST_IsEmpty({g}))").format(
            g=g, t=table
        ),
    )
    if still_bad:
        problems.append(f"{layer}: {still_bad} geometries invalid or empty after ST_MakeValid")

    degrees = g if srid == SRID else sql.SQL("ST_Transform({g}, {srid})").format(g=g, srid=sql.Literal(SRID))
    xmin, ymin, xmax, ymax = _one(
        conn,
        sql.SQL("SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM (SELECT ST_Extent({d}) AS e FROM {t}) s").format(
            d=degrees, t=table
        ),
    )
    if xmin is None:
        return None, problems or [f"{layer}: no geometries"]
    extent = BBox(float(xmin), float(ymin), float(xmax), float(ymax))
    if not _inside(extent, bbox, EXTENT_TOLERANCE_DEG if srid == SRID else REPROJECTED_TOLERANCE_DEG):
        problems.append(f"{layer}: extent {extent.as_dict()} is not inside the package bbox {bbox.as_dict()}")

    return LayerReport(layer=layer, rows=rows, repaired=repaired, extent=extent), problems


def check_staging(
    conn: psycopg.Connection[Any],
    schema: str,
    layers: Sequence[str],
    bbox: BBox,
    geom: str = "geom",
    srid: int = SRID,
) -> list[LayerReport]:
    """Check and repair every layer in schema. Raises StagingCheckFailed listing all problems.

    Runs in the caller's transaction and does not commit: the repairs belong to the same
    transaction as the swap, and roll back with it.
    """
    if not layers:
        raise StagingCheckFailed("no layers to check")
    reports: list[LayerReport] = []
    problems: list[str] = []
    for layer in layers:
        report, layer_problems = check_layer(conn, schema, layer, bbox, geom, srid)
        problems += layer_problems
        if report is not None:
            reports.append(report)
            log.info(
                "%s.%s: %d rows, %d repaired, extent %s", schema, layer, report.rows, report.repaired, report.extent
            )
    if problems:
        raise StagingCheckFailed("staging checks failed, not swapping:\n  " + "\n  ".join(problems))
    return reports
