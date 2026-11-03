"""Fill dvw.ais_1min and dvw.ais_1hour from the rows already in dvw.tracks.

    uv run python -m sql.backfill_rollups              (make backfill-rollups)
    uv run python -m sql.backfill_rollups --run <id>   one run only

A materialised view only sees rows inserted after it was created, so tracks stored
before migration 0004 are not in the rollups. This inserts them using each view's own
SELECT (read from system.tables, so the backfill cannot drift from the view), one run
and source partition at a time through additional_table_filters on dvw.tracks. One
chunk is about a sixth of a run: a whole 413k-fix run in one GROUP BY took 313 MiB,
a quarter of ClickHouse's memory limit.

Safe to run again, and while the tracks sink is inserting: every rollup aggregate gives
the same result when a fix is added twice (sql/clickhouse/0004, docs/adr/0020).
"""

from __future__ import annotations

import argparse
import sys
import time

from clickhouse_connect.driver.client import Client

from common.clickhouse import client_from_env

VIEWS = {"dvw.ais_1min": "ais_1min_mv", "dvw.ais_1hour": "ais_1hour_mv"}
# One thread, and spill to disk past 100 MB rather than risk the 1.25 GB container.
SETTINGS = {"max_threads": 1, "max_bytes_before_external_group_by": 100_000_000}
PARTITIONS = range(6)  # ADR 0001: every topic has 6 partitions


def view_select(client: Client, view: str) -> str:
    # query(), not command(): command() returns the text TSV-escaped ('' as \'\').
    rows = client.query(
        "SELECT as_select FROM system.tables WHERE database = 'dvw' AND name = {v:String}", parameters={"v": view}
    ).result_rows
    if not rows or not str(rows[0][0]).strip():
        raise SystemExit(f"dvw.{view} not found. Run: make migrate")
    return str(rows[0][0])


def quote(s: str) -> str:
    """A ClickHouse single-quoted string literal."""
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def runs(client: Client) -> list[str]:
    q = client.query("SELECT DISTINCT run_id FROM dvw.tracks WHERE decode_error = '' ORDER BY run_id")
    return [str(r[0]) for r in q.result_rows]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="backfill_rollups", description=__doc__.splitlines()[0])
    p.add_argument("--run", help="only this run_id (default: every run in dvw.tracks)")
    args = p.parse_args(argv)
    client = client_from_env()
    selects = {table: view_select(client, view) for table, view in VIEWS.items()}
    todo = [args.run] if args.run else runs(client)
    for run_id in todo:
        t0 = time.monotonic()
        for part in PARTITIONS:
            cond = f"run_id = {quote(run_id)} AND src_partition = {part}"
            for table, select in selects.items():
                client.command(
                    f"INSERT INTO {table} {select}",
                    settings={**SETTINGS, "additional_table_filters": f"{{'dvw.tracks': {quote(cond)}}}"},
                )
        print(f"backfill: {run_id} in {time.monotonic() - t0:.1f}s")
    print(f"backfill: {len(todo)} runs done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
