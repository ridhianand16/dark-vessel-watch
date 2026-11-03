"""Load a verified zone package into PostGIS by swapping it in from a staging schema (ADR 0010).

    uv run python -m refdata load [--package DIR]     # or: make refdata-load

One transaction:

    1. verify the package (refdata/package.py); nothing touches the database before;
    2. create refdata_staging with copies of the live tables (LIKE ... INCLUDING ALL,
       so columns, constraints and indexes come from the migrations);
    3. insert every layer, then check: row counts equal the manifest's, and the
       staging checks pass (refdata/staging.py);
    4. drop the live tables and move the staging ones in, then record the load in
       refdata_packages.

Any failure rolls everything back and the live zones stay as they were. Readers are
only blocked from step 4 to the commit. Reloading the same package replaces the rows
with identical ones and adds a refdata_packages row.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from common.postgis import conninfo
from refdata import gpkg
from refdata.area import BBox
from refdata.build import AREA, read_area
from refdata.package import PUBLIC_KEY, TABLES, VerifiedPackage, verify_package
from refdata.staging import StagingCheckFailed, check_staging

STAGING = "refdata_staging"
LIVE = "public"
SRID = 4326
LOCK_TIMEOUT = "5s"  # give up on the swap rather than queue readers behind a long query


@dataclass(frozen=True)
class LoadReport:
    version: str
    rows: dict[str, int]
    verify_s: float
    load_s: float
    swap_ms: float


def _insert_layer(conn: psycopg.Connection[Any], pkg: VerifiedPackage, name: str) -> list[str]:
    entry = next(f for f in pkg.files if f.name == name)
    layer = entry.layer
    columns = [*layer.columns, "package_version", "geom"]
    query = sql.SQL("INSERT INTO {t} ({cols}) VALUES ({vals}, ST_Multi(ST_GeomFromWKB(%s, {srid})))").format(
        t=sql.Identifier(STAGING, layer.table),
        cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
        vals=sql.SQL(", ").join(sql.Placeholder() * (len(layer.columns) + 1)),
        srid=sql.Literal(SRID),
    )
    problems: list[str] = []
    rows: list[tuple[Any, ...]] = []
    for *attrs, srs_id, wkb in gpkg.read_features(pkg.path / name, layer.name, list(layer.columns)):
        if wkb is None:
            problems.append(f"{name}: {attrs[0]} has an empty geometry")
        elif srs_id != SRID:
            problems.append(f"{name}: {attrs[0]} is in SRS {srs_id}, not {SRID}")
        else:
            rows.append((*attrs, pkg.version, wkb))
    with conn.cursor() as cur:
        cur.executemany(query, rows)
    return problems


def _count(conn: psycopg.Connection[Any], schema: str, table: str) -> int:
    row = conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(schema, table))).fetchone()
    assert row is not None
    return int(row[0])


def load(package: Path, public_key: Path = PUBLIC_KEY, area: Path = AREA) -> LoadReport:
    t0 = time.monotonic()
    pkg = verify_package(package, public_key)
    t_verified = time.monotonic()
    print(f"load: {pkg.version} verified (manifest sha256 {pkg.manifest_sha256[:12]})", file=sys.stderr)

    b = read_area(area)["bbox"]
    bbox = BBox(float(b["min_lon"]), float(b["min_lat"]), float(b["max_lon"]), float(b["max_lat"]))
    if pkg.bbox != bbox:
        raise StagingCheckFailed(
            f"package bbox {pkg.bbox.as_dict()} differs from {area.name} {bbox.as_dict()}: "
            "it was built for a different area"
        )

    with psycopg.connect(conninfo()) as conn:  # one transaction; commits on success
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(STAGING)))
        for table in TABLES:
            conn.execute(
                sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
                    sql.Identifier(STAGING, table), sql.Identifier(LIVE, table)
                )
            )
        problems: list[str] = []
        for entry in pkg.files:
            problems += _insert_layer(conn, pkg, entry.name)
        rows = {table: _count(conn, STAGING, table) for table in TABLES}
        for table in TABLES:
            if rows[table] != pkg.features(table):
                problems.append(f"{table}: {rows[table]} rows loaded, manifest lists {pkg.features(table)}")
        if problems:
            raise StagingCheckFailed("package rows rejected, not swapping:\n  " + "\n  ".join(problems))
        check_staging(conn, STAGING, TABLES, bbox)
        for table in TABLES:
            conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(STAGING, table)))

        t_swap = time.monotonic()
        conn.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(LOCK_TIMEOUT)))
        for table in TABLES:
            conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(LIVE, table)))
            conn.execute(
                sql.SQL("ALTER TABLE {} SET SCHEMA {}").format(sql.Identifier(STAGING, table), sql.Identifier(LIVE))
            )
        conn.execute(sql.SQL("DROP SCHEMA {}").format(sql.Identifier(STAGING)))
        conn.execute(
            "INSERT INTO refdata_packages (package_version, manifest_sha256, verified_at) VALUES (%s, %s, %s)",
            (pkg.version, pkg.manifest_sha256, pkg.verified_at),
        )
    t_done = time.monotonic()
    return LoadReport(
        version=pkg.version,
        rows=rows,
        verify_s=t_verified - t0,
        load_s=t_swap - t_verified,
        swap_ms=(t_done - t_swap) * 1000,
    )
