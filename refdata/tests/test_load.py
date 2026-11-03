"""The loader against a scratch database on the compose PostGIS (skipped when unreachable)."""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from common.postgis import conninfo
from refdata.area import BBox
from refdata.load import load
from refdata.package import VerifyError
from refdata.staging import StagingCheckFailed
from refdata.tests.fake_package import default_rows, gpkg_blob, make_package, multipolygon, square, write_area
from refdata.tests.minisign_keys import SigningKey
from sql.migrate import main as migrate

BBOX = BBox(2.6909, 53.5224, 17.5334, 59.5686)


@pytest.fixture
def db(scratch_db: str) -> str:
    assert migrate(["--target", "postgis"]) == 0
    return scratch_db


@pytest.fixture
def key() -> SigningKey:
    return SigningKey()


@pytest.fixture
def public_key(tmp_path: Path, key: SigningKey) -> Path:
    path = tmp_path / "dvw.pub"
    path.write_text(key.public_text())
    return path


@pytest.fixture
def area(tmp_path: Path) -> Path:
    return write_area(tmp_path / "area.yaml", BBOX)


def live() -> tuple[object, ...]:
    """Fingerprint of the live tables and the load registry.

    (zones md5, zones, eez_boundaries, zones_25832, eez_boundaries_25832,
     zones_25832 md5, active package, registry rows, staging schemas)
    """
    with psycopg.connect(conninfo()) as conn:
        row = conn.execute(
            "SELECT (SELECT md5(coalesce(string_agg(zone_id || package_version || ST_AsEWKB(geom)::text, ','"
            "        ORDER BY zone_id), '')) FROM zones),"
            " (SELECT count(*) FROM zones), (SELECT count(*) FROM eez_boundaries),"
            " (SELECT count(*) FROM zones_25832), (SELECT count(*) FROM eez_boundaries_25832),"
            " (SELECT md5(coalesce(string_agg(zone_id || package_version || ST_AsEWKB(geom)::text, ','"
            "        ORDER BY zone_id), '')) FROM zones_25832),"
            " (SELECT package_version FROM refdata_packages WHERE active),"
            " (SELECT count(*) FROM refdata_packages),"
            " (SELECT count(*) FROM pg_namespace WHERE nspname = 'refdata_staging')"
        ).fetchone()
    assert row is not None
    return tuple(row)


def test_loading_twice_gives_same_rows_and_two_registry_rows(
    db: str, tmp_path: Path, key: SigningKey, public_key: Path, area: Path
) -> None:
    pkg = make_package(tmp_path / "packages", key, BBOX)
    first = load(pkg, public_key, area)
    after_first = live()
    second = load(pkg, public_key, area)
    assert first.rows == second.rows == {"zones": 4, "eez_boundaries": 1, "zones_25832": 4, "eez_boundaries_25832": 1}
    assert live()[:7] == after_first[:7]
    assert live()[6:] == ("zones-test00000001", 2, 0)  # two loads recorded, no staging schema left
    with psycopg.connect(conninfo()) as conn:
        kinds: dict[str, int] = dict(conn.execute("SELECT kind, count(*) FROM zones GROUP BY kind").fetchall())
        indexes = {
            r[0]
            for r in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE tablename IN ('zones', 'zones_25832', 'eez_boundaries_25832')"
            )
        }
        (srids,) = conn.execute(
            "SELECT array[(SELECT DISTINCT ST_SRID(geom) FROM zones_25832), (SELECT DISTINCT ST_SRID(geom)"
            " FROM eez_boundaries_25832)]"
        ).fetchone() or (None,)
        (active,) = conn.execute("SELECT count(*) FROM refdata_packages WHERE active").fetchone() or (None,)
    assert kinds == {"eez": 1, "mpa": 2, "port": 1}
    assert indexes == {  # carried over by the swap
        "zones_pkey", "zones_geom_idx", "zones_kind_idx",
        "zones_25832_pkey", "zones_25832_geom_idx", "zones_25832_kind_idx",
        "eez_boundaries_25832_pkey", "eez_boundaries_25832_geom_idx",
    }  # fmt: skip
    assert srids == [25832, 25832]
    assert active == 1


def test_newest_load_is_the_active_package(
    db: str, tmp_path: Path, key: SigningKey, public_key: Path, area: Path
) -> None:
    load(make_package(tmp_path / "a", key, BBOX), public_key, area)
    load(make_package(tmp_path / "b", key, BBOX, version="zones-test00000002"), public_key, area)
    with psycopg.connect(conninfo()) as conn:
        assert conn.execute("SELECT package_version FROM refdata_packages WHERE active").fetchall() == [
            ("zones-test00000002",)
        ]
        with pytest.raises(psycopg.errors.UniqueViolation):  # never two active rows
            conn.execute("UPDATE refdata_packages SET active = true")


def test_metric_copy_distances_match_geography(
    db: str, tmp_path: Path, key: SigningKey, public_key: Path, area: Path
) -> None:
    load(make_package(tmp_path / "p", key, BBOX), public_key, area)
    with psycopg.connect(conninfo()) as conn:
        row = conn.execute(
            "SELECT ST_Distance(ST_Transform(ST_SetSRID(ST_Point(11.2, 56.6), 4326), 25832), m.geom),"
            "       ST_Distance(ST_SetSRID(ST_Point(11.2, 56.6), 4326)::geography, g.geom::geography)"
            " FROM eez_boundaries_25832 m JOIN eez_boundaries g USING (id)"
        ).fetchone()
    assert row is not None
    metric, geodesic = row
    assert 20_000 < geodesic < 30_000
    # The boundary is one ~127 km segment; without densifying, the metric copy is a straight
    # line in UTM and misses by tens of metres. What is left is UTM 32N's scale error near
    # 11 E, about 0.01 %.
    assert abs(metric - geodesic) < 0.0002 * geodesic


def test_zone_along_the_bbox_edge_stays_inside_it_in_the_metric_copy(
    db: str, tmp_path: Path, key: SigningKey, public_key: Path, area: Path
) -> None:
    # A polygon clipped to the bbox's northern parallel, 10 degrees wide. Densified as a
    # geodesic its top edge would bow ~700 m north of the bbox and fail the extent check.
    top = BBOX.max_lat
    strip = [(5.0, top - 0.2), (15.0, top - 0.2), (15.0, top), (5.0, top), (5.0, top - 0.2)]
    rows = default_rows()
    rows["mpa"].append(("wdpa:strip", "mpa", "Strip", "wdpa_marine:Nov2026", gpkg_blob(multipolygon(strip))))
    report = load(make_package(tmp_path / "p", key, BBOX, rows=rows), public_key, area)
    assert report.rows["zones_25832"] == 5


def test_invalid_zone_is_repaired_before_the_metric_copy(
    db: str, tmp_path: Path, key: SigningKey, public_key: Path, area: Path
) -> None:
    rows = default_rows()
    bowtie = [(11.0, 57.0), (11.2, 57.2), (11.2, 57.0), (11.0, 57.2), (11.0, 57.0)]  # self-intersecting
    rows["mpa"].append(("wdpa:bowtie", "mpa", "Bowtie", "wdpa_marine:Nov2026", gpkg_blob(multipolygon(bowtie))))
    load(make_package(tmp_path / "p", key, BBOX, rows=rows), public_key, area)
    with psycopg.connect(conninfo()) as conn:
        row = conn.execute(
            "SELECT bool_and(ST_IsValid(geom)), count(*) FROM zones_25832 WHERE zone_id = 'wdpa:bowtie'"
        ).fetchone()
    assert row == (True, 1)


def test_tampered_package_leaves_live_tables_unchanged(
    db: str, tmp_path: Path, key: SigningKey, public_key: Path, area: Path
) -> None:
    load(make_package(tmp_path / "a", key, BBOX), public_key, area)
    before = live()
    pkg = make_package(tmp_path / "b", key, BBOX, version="zones-test00000002")
    data = bytearray((pkg / "eez.gpkg").read_bytes())
    data[200] ^= 0x01
    (pkg / "eez.gpkg").write_bytes(bytes(data))
    with pytest.raises(VerifyError, match="eez.gpkg: SHA-256"):
        load(pkg, public_key, area)
    assert live() == before


def test_failed_staging_check_rolls_back(
    db: str, tmp_path: Path, key: SigningKey, public_key: Path, area: Path
) -> None:
    load(make_package(tmp_path / "a", key, BBOX), public_key, area)
    before = live()
    rows = default_rows()
    rows["mpa"].append(("wdpa:far", "mpa", "Outside", "wdpa_marine:Nov2026", gpkg_blob(multipolygon(square(20, 60)))))
    pkg = make_package(tmp_path / "b", key, BBOX, version="zones-test00000003", rows=rows)
    with pytest.raises(StagingCheckFailed, match="zones: extent"):
        load(pkg, public_key, area)
    assert live() == before


def test_package_for_another_area_is_refused(
    db: str, tmp_path: Path, key: SigningKey, public_key: Path, area: Path
) -> None:
    pkg = make_package(tmp_path / "p", key, BBox(0, 50, 20, 60))
    with pytest.raises(StagingCheckFailed, match="built for a different area"):
        load(pkg, public_key, area)
    assert live()[1:5] == (0, 0, 0, 0)
    assert live()[6:8] == (None, 0)
