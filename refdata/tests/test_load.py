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
    """Fingerprint of the live tables and the load registry."""
    with psycopg.connect(conninfo()) as conn:
        row = conn.execute(
            "SELECT (SELECT md5(coalesce(string_agg(zone_id || package_version || ST_AsEWKB(geom)::text, ','"
            "        ORDER BY zone_id), '')) FROM zones),"
            " (SELECT count(*) FROM zones), (SELECT count(*) FROM eez_boundaries),"
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
    assert first.rows == second.rows == {"zones": 4, "eez_boundaries": 1}
    assert live()[:3] == after_first[:3]
    assert live()[3:] == (2, 0)  # two loads recorded, no staging schema left
    with psycopg.connect(conninfo()) as conn:
        kinds: dict[str, int] = dict(conn.execute("SELECT kind, count(*) FROM zones GROUP BY kind").fetchall())
        indexes = {r[0] for r in conn.execute("SELECT indexname FROM pg_indexes WHERE tablename = 'zones'")}
    assert kinds == {"eez": 1, "mpa": 2, "port": 1}
    assert indexes == {"zones_pkey", "zones_geom_idx", "zones_kind_idx"}  # carried over by the swap


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
    assert live()[1:4] == (0, 0, 0)
