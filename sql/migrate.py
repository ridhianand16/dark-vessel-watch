"""Apply numbered ClickHouse migrations in order and record them.

    uv run python -m sql.migrate            # or: make migrate
    uv run python -m sql.migrate --dry-run  # show the plan, change nothing

Files are sql/clickhouse/NNNN_name.sql. Each applied file is recorded in
dvw.schema_migrations with the SHA-256 of its bytes. The run fails, before applying
anything, if an applied file was edited, renamed or deleted, or if a new file is
numbered below the latest applied one.

ClickHouse DDL is not transactional: if a file fails halfway, its earlier statements
stay applied and the file is not recorded, so the next run re-executes it from the top.
Write every statement so that re-running it is harmless (IF NOT EXISTS and similar).
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from clickhouse_connect.driver.client import Client

from common.clickhouse import client_from_env
from common.env import REPO_ROOT

DEFAULT_DIR = REPO_ROOT / "sql" / "clickhouse"
TABLE = "dvw.schema_migrations"
FILE_RE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")


class MigrationError(Exception):
    pass


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


def table_exists(client: Client) -> bool:
    return bool(client.command(f"EXISTS TABLE {TABLE}"))


def as_text(value: object) -> str:
    """clickhouse-connect returns FixedString as bytes; str(b"..") would give "b'..'"."""
    return value.decode("ascii") if isinstance(value, bytes) else str(value)


def read_applied(client: Client) -> list[Applied]:
    rows = client.query(f"SELECT version, name, checksum FROM {TABLE} ORDER BY version").result_rows
    return [Applied(int(v), as_text(n), as_text(c)) for v, n, c in rows]


def apply(client: Client, m: Migration) -> None:
    for stmt in split_statements(m.sql):
        client.command(stmt)
    client.insert(TABLE, [[m.version, m.name, m.checksum]], column_names=["version", "name", "checksum"])


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="migrate", description=__doc__.splitlines()[0])
    p.add_argument("--dir", type=Path, default=DEFAULT_DIR)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    try:
        migrations = discover(args.dir)
        if not migrations or migrations[0].version != 0:
            raise MigrationError(f"{args.dir}: 0000_*.sql must exist and create {TABLE}")
        client = client_from_env()
        if not table_exists(client):
            if args.dry_run:
                applied: list[Applied] = []
            else:
                # Bootstrap: 0000 creates the bookkeeping table. It is recorded below
                # like any other file (re-running it is a no-op by design).
                for stmt in split_statements(migrations[0].sql):
                    client.command(stmt)
                applied = read_applied(client)
        else:
            applied = read_applied(client)
        pending = plan(migrations, applied)
    except MigrationError as e:
        print(f"migrate: ERROR: {e}", file=sys.stderr)
        return 1

    if not pending:
        print(f"migrate: up to date ({len(applied)} applied, nothing to do)")
        return 0
    for m in pending:
        label = f"{m.version:04d}_{m.name}"
        if args.dry_run:
            print(f"migrate: would apply {label} sha256={m.checksum[:12]}")
            continue
        apply(client, m)
        print(f"migrate: applied {label} sha256={m.checksum[:12]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
