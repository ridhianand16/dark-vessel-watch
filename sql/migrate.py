"""Apply numbered migrations to ClickHouse or PostGIS, in order, and record them.

    uv run python -m sql.migrate                     # ClickHouse (make migrate)
    uv run python -m sql.migrate --target postgis    # PostGIS (make migrate-postgis)
    uv run python -m sql.migrate --dry-run           # show the plan, change nothing

Files are sql/<target>/NNNN_name.sql. Each applied file is recorded in the target's
schema_migrations table with the SHA-256 of its bytes. The run fails, before applying
anything, if an applied file was edited, renamed or deleted, or if a new file is
numbered below the latest applied one. The rules are the same for both targets.

ClickHouse DDL is not transactional: if a file fails halfway, its earlier statements
stay applied and the file is not recorded, so the next run re-executes it from the top.
Write every ClickHouse statement so that re-running it is harmless (IF NOT EXISTS and
similar).

PostgreSQL DDL is transactional: each PostGIS file runs as one transaction together
with its schema_migrations row, so a file is either fully applied and recorded or not
at all. The file is sent as one script, so dollar quoting and functions work.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable
from typing import Any, Protocol

import psycopg
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from common.clickhouse import client_from_env
from common.env import REPO_ROOT
from common.postgis import conninfo

SQL_ROOT = REPO_ROOT / "sql"
TARGETS = ("clickhouse", "postgis")
FILE_RE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")


class MigrationError(Exception):
    pass


# Errors from the database itself (unreachable, a statement it rejected): reported as one
# ERROR line like a plan error, not as a traceback.
DATABASE_ERRORS = (psycopg.Error, ClickHouseError)


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum: str


@dataclass(frozen=True)
class Applied:
    version: int
    name: str
    checksum: str


def discover(directory: Path) -> list[Migration]:
    out: dict[int, Migration] = {}
    for path in sorted(directory.glob("*.sql")):
        m = FILE_RE.match(path.name)
        if m is None:
            raise MigrationError(f"{path.name}: expected NNNN_lowercase_name.sql")
        version = int(m.group(1))
        if version in out:
            raise MigrationError(f"{path.name}: version {version:04d} is used twice")
        data = path.read_bytes()
        out[version] = Migration(version, m.group(2), data.decode("utf-8"), hashlib.sha256(data).hexdigest())
    return [out[v] for v in sorted(out)]


def plan(migrations: list[Migration], applied: list[Applied]) -> list[Migration]:
    """Return the migrations still to apply, or raise if history and files disagree."""
    on_disk = {m.version: m for m in migrations}
    for a in applied:
        m = on_disk.get(a.version)
        label = f"{a.version:04d}_{a.name}"
        if m is None:
            raise MigrationError(f"{label} is applied but its file is missing")
        if m.name != a.name:
            raise MigrationError(f"{label} is applied but the file is now named {m.version:04d}_{m.name}")
        if m.checksum != a.checksum:
            raise MigrationError(
                f"{label}.sql changed after it was applied (sha256 {a.checksum[:12]} -> {m.checksum[:12]}). "
                "Revert it and add a new numbered migration instead."
            )
    done = {a.version for a in applied}
    latest = max(done, default=-1)
    pending = [m for m in migrations if m.version not in done]
    for m in pending:
        if m.version < latest:
            raise MigrationError(
                f"{m.version:04d}_{m.name} is new but numbered below the latest applied "
                f"migration {latest:04d}. Renumber it above {latest:04d}."
            )
    return pending


def split_statements(sql: str) -> list[str]:
    """Split on top-level semicolons, ignoring those in quotes and comments.

    Comment-only fragments are dropped. Comments inside a statement are kept, since
    ClickHouse accepts them.
    """
    statements: list[str] = []
    buf: list[str] = []
    has_code = False
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "-" and sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j
            buf.append(sql[i:j])
            i = j
        elif c == "/" and sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            if j == -1:
                raise MigrationError("unterminated /* comment")
            buf.append(sql[i : j + 2])
            i = j + 2
        elif c in "'\"`":
            j = i + 1
            while True:
                if j >= n:
                    raise MigrationError(f"unterminated {c} quote")
                if sql[j] == "\\":
                    j += 2
                elif sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # doubled quote escapes itself
                        j += 2
                    else:
                        break
                else:
                    j += 1
            buf.append(sql[i : j + 1])
            has_code = True
            i = j + 1
        elif c == ";":
            if has_code:
                statements.append("".join(buf).strip())
            buf, has_code = [], False
            i += 1
        else:
            buf.append(c)
            has_code = has_code or not c.isspace()
            i += 1
    if has_code:
        statements.append("".join(buf).strip())
    return statements


class Target(Protocol):
    table: str

    def has_table(self) -> bool: ...

    def read_applied(self) -> list[Applied]: ...

    def run(self, m: Migration) -> None:
        """Execute a file's SQL without recording it (bootstrapping 0000)."""
        ...

    def apply(self, m: Migration) -> None:
        """Execute a file's SQL and record it in table."""
        ...

    def close(self) -> None: ...


def as_text(value: object) -> str:
    """clickhouse-connect returns FixedString as bytes; str(b"..") would give "b'..'"."""
    return value.decode("ascii") if isinstance(value, bytes) else str(value)


class ClickHouseTarget:
    table = "dvw.schema_migrations"

    def __init__(self, client: Client) -> None:
        self.client = client

    def has_table(self) -> bool:
        return bool(self.client.command(f"EXISTS TABLE {self.table}"))

    def read_applied(self) -> list[Applied]:
        rows = self.client.query(f"SELECT version, name, checksum FROM {self.table} ORDER BY version").result_rows
        return [Applied(int(v), as_text(n), as_text(c)) for v, n, c in rows]

    def run(self, m: Migration) -> None:
        for stmt in split_statements(m.sql):
            self.client.command(stmt)

    def apply(self, m: Migration) -> None:
        self.run(m)
        self.client.insert(self.table, [[m.version, m.name, m.checksum]], column_names=["version", "name", "checksum"])

    def close(self) -> None:
        self.client.close()


class PostgisTarget:
    table = "public.schema_migrations"

    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        conn.autocommit = True  # every file gets its own explicit transaction
        self.conn = conn

    def has_table(self) -> bool:
        row = self.conn.execute("SELECT to_regclass(%s) IS NOT NULL", (self.table,)).fetchone()
        return bool(row and row[0])

    def read_applied(self) -> list[Applied]:
        rows = self.conn.execute("SELECT version, name, checksum FROM public.schema_migrations ORDER BY version")
        return [Applied(int(v), str(n), str(c)) for v, n, c in rows.fetchall()]

    def run(self, m: Migration) -> None:
        with self.conn.transaction():
            # Without parameters psycopg sends the file as one simple query, so it may
            # hold several statements.
            self.conn.execute(m.sql.encode("utf-8"))

    def apply(self, m: Migration) -> None:
        with self.conn.transaction():
            self.conn.execute(m.sql.encode("utf-8"))
            self.conn.execute(
                "INSERT INTO public.schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
                (m.version, m.name, m.checksum),
            )

    def close(self) -> None:
        self.conn.close()


def connect(target: str) -> Target:
    if target == "clickhouse":
        return ClickHouseTarget(client_from_env())
    return PostgisTarget(psycopg.connect(conninfo()))


def main(argv: list[str] | None = None, connect: Callable[[str], Target] = connect) -> int:
    p = argparse.ArgumentParser(prog="migrate", description=__doc__.splitlines()[0])
    p.add_argument("--target", choices=TARGETS, default="clickhouse")
    p.add_argument("--dir", type=Path, help="migrations directory (default: sql/<target>)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)
    directory: Path = args.dir or SQL_ROOT / args.target
    tag = f"migrate[{args.target}]"

    db: Target | None = None
    label = ""  # the file being applied, for the error message
    try:
        migrations = discover(directory)
        if not migrations or migrations[0].version != 0:
            raise MigrationError(f"{directory}: 0000_*.sql must exist and create the schema_migrations table")
        db = connect(args.target)
        if not db.has_table():
            if args.dry_run:
                applied: list[Applied] = []
            else:
                # Bootstrap: 0000 creates the bookkeeping table. It is recorded below
                # like any other file (re-running it is a no-op by design).
                label = f"{migrations[0].version:04d}_{migrations[0].name}"
                db.run(migrations[0])
                applied = db.read_applied()
        else:
            applied = db.read_applied()
        pending = plan(migrations, applied)
        if not pending:
            print(f"{tag}: up to date ({len(applied)} applied, nothing to do)")
            return 0
        for m in pending:
            label = f"{m.version:04d}_{m.name}"
            if args.dry_run:
                print(f"{tag}: would apply {label} sha256={m.checksum[:12]}")
                continue
            db.apply(m)
            print(f"{tag}: applied {label} sha256={m.checksum[:12]}")
        return 0
    except MigrationError as e:
        print(f"{tag}: ERROR: {e}", file=sys.stderr)
        return 1
    except DATABASE_ERRORS as e:
        where = f"{label} failed and is not recorded" if label else "cannot read the migration state"
        print(f"{tag}: ERROR: {where}: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    sys.exit(main())
