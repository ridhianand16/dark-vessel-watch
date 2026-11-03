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
    assert {"schema_migrations", "zones", "eez_boundaries", "refdata_packages", "vessels_current"} <= tables()
    assert main(["--target", "postgis"]) == 0
    assert "up to date (3 applied" in capsys.readouterr().out


def test_failing_file_leaves_nothing_behind(scratch_db: str, tmp_path: Path) -> None:
    (tmp_path / "0000_migrations.sql").write_bytes((REPO_ROOT / "sql" / "postgis" / "0000_migrations.sql").read_bytes())
    (tmp_path / "0001_half.sql").write_text("CREATE TABLE half_done (x int);\nSELECT no_such_function();\n")
    with pytest.raises(psycopg.errors.UndefinedFunction):
        main(["--target", "postgis", "--dir", str(tmp_path)])
    # The file ran in one transaction with its bookkeeping row: neither survived.
    assert "half_done" not in tables()
    with psycopg.connect(conninfo()) as conn:
        assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone() == (1,)
