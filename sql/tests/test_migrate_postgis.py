"""sql.migrate --target postgis against a scratch database on the compose PostGIS."""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from common.env import REPO_ROOT
from common.postgis import conninfo
from sql.migrate import main


def tables() -> set[str]:
    with psycopg.connect(conninfo()) as conn:
        rows = conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'").fetchall()
    return {r[0] for r in rows}


def test_applies_once_then_is_up_to_date(scratch_db: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--target", "postgis"]) == 0
    out = capsys.readouterr().out
    assert "applied 0001_zone_package" in out and "applied 0002_vessels_current" in out
    assert "applied 0003_zone_metric_copy" in out and "applied 0004_zone_pieces" in out
    assert {"schema_migrations", "zones", "eez_boundaries", "refdata_packages", "vessels_current",
            "zones_25832", "eez_boundaries_25832",
            "zone_pieces_25832", "eez_boundary_pieces_25832"} <= tables()  # fmt: skip
    assert main(["--target", "postgis"]) == 0
    assert "up to date (5 applied" in capsys.readouterr().out


def test_failing_file_leaves_nothing_behind(
    scratch_db: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "0000_migrations.sql").write_bytes((REPO_ROOT / "sql" / "postgis" / "0000_migrations.sql").read_bytes())
    (tmp_path / "0001_half.sql").write_text("CREATE TABLE half_done (x int);\nSELECT no_such_function();\n")
    assert main(["--target", "postgis", "--dir", str(tmp_path)]) == 1
    assert "ERROR: 0001_half failed and is not recorded: UndefinedFunction" in capsys.readouterr().err
    # The file ran in one transaction with its bookkeeping row: neither survived.
    assert "half_done" not in tables()
    with psycopg.connect(conninfo()) as conn:
        assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone() == (1,)
