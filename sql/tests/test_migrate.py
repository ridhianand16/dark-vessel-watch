from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from sql.migrate import Applied, Migration, MigrationError, as_text, discover, plan, split_statements


def mig(version: int, name: str, sql: str = "SELECT 1") -> Migration:
    return Migration(version, name, sql, hashlib.sha256(sql.encode()).hexdigest())


def applied(m: Migration) -> Applied:
    return Applied(m.version, m.name, m.checksum)


def test_plan_returns_unapplied_in_order() -> None:
    a, b, c = mig(0, "migrations"), mig(1, "ais_raw"), mig(2, "alerts")
    assert plan([a, b, c], [applied(a)]) == [b, c]


def test_plan_is_empty_when_everything_is_applied() -> None:
    a, b = mig(0, "migrations"), mig(1, "ais_raw")
    assert plan([a, b], [applied(a), applied(b)]) == []


def test_plan_rejects_edited_applied_file() -> None:
    old, new = mig(1, "ais_raw", "CREATE TABLE x"), mig(1, "ais_raw", "CREATE TABLE y")
    with pytest.raises(MigrationError, match="changed after it was applied"):
        plan([new], [applied(old)])


def test_plan_rejects_missing_or_renamed_file() -> None:
    a = mig(1, "ais_raw")
    with pytest.raises(MigrationError, match="file is missing"):
        plan([], [applied(a)])
    with pytest.raises(MigrationError, match="now named"):
        plan([mig(1, "raw")], [applied(a)])


def test_plan_rejects_new_file_below_latest_applied() -> None:
    a, c = mig(0, "migrations"), mig(2, "alerts")
    with pytest.raises(MigrationError, match="numbered below"):
        plan([a, mig(1, "late"), c], [applied(a), applied(c)])


def test_discover_checks_names_and_hashes_bytes(tmp_path: Path) -> None:
    (tmp_path / "0001_b.sql").write_bytes(b"SELECT 2;\n")
    (tmp_path / "0000_a.sql").write_bytes(b"SELECT 1;\n")
    ms = discover(tmp_path)
    assert [(m.version, m.name) for m in ms] == [(0, "a"), (1, "b")]
    assert ms[1].checksum == hashlib.sha256(b"SELECT 2;\n").hexdigest()
    (tmp_path / "notes.sql").write_text("")
    with pytest.raises(MigrationError, match="NNNN_lowercase_name"):
        discover(tmp_path)


def test_split_ignores_semicolons_in_comments_and_quotes() -> None:
    sql = """
    -- header; with a semicolon
    CREATE TABLE a (s String DEFAULT 'x;y', t String DEFAULT 'it''s; \\'ok\\'');
    /* block; comment */
    CREATE TABLE `b;c` (x UInt8) -- trailing; comment
    ;
    -- only a comment at the end;
    """
    stmts = split_statements(sql)
    assert len(stmts) == 2
    assert stmts[0].endswith("DEFAULT 'it''s; \\'ok\\'')")
    assert "`b;c`" in stmts[1]


def test_split_rejects_unterminated_quote() -> None:
    with pytest.raises(MigrationError, match="unterminated"):
        split_statements("SELECT 'oops;")


def test_fixedstring_checksum_from_driver_compares_equal() -> None:
    # clickhouse-connect returns FixedString(64) as bytes. Regression: str(bytes)
    # produced "b'..'" and every applied file looked edited on the second run.
    m = mig(0, "migrations")
    from_db = Applied(0, as_text("migrations"), as_text(m.checksum.encode("ascii")))
    assert plan([m], [from_db]) == []
