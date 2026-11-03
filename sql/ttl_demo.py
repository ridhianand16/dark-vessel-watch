"""Show the ingested_at TTL deleting rows, on a scratch table. Changes no real table.

    uv run python -m sql.ttl_demo        (make ttl-demo)

Creates dvw.ttl_demo with the TTL expression the real tables use (sql/clickhouse/0005),
shortened from days to minutes, inserts rows of which some have ingested_at backdated
past the TTL, forces a merge with OPTIMIZE ... FINAL (TTL deletes during merges), shows
which rows are left, and drops the table.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta

from clickhouse_connect.driver.client import Client

from common.clickhouse import client_from_env

TABLE = "dvw.ttl_demo"
TTL_MINUTES = 5
REAL_TABLES = ("ais_raw", "tracks", "ais_1min", "ais_1hour")


def show_real_ttls(client: Client) -> None:
    print("TTL on the real tables (from system.tables):")
    for name in REAL_TABLES:
        engine_full = str(client.command(f"SELECT engine_full FROM system.tables WHERE database = 'dvw' AND name = '{name}'"))
        ttl = engine_full.split(" TTL ", 1)[1].split(" SETTINGS ", 1)[0] if " TTL " in engine_full else "none (kept)"
        print(f"  {name:<10} {ttl}")


def rows(client: Client) -> list[tuple[str, datetime, datetime]]:
    q = client.query(f"SELECT label, event_ts, ingested_at FROM {TABLE} ORDER BY label")
    return [(str(a), b, c) for a, b, c in q.result_rows]


def print_rows(title: str, data: list[tuple[str, datetime, datetime]]) -> None:
    print(f"{title}: {len(data)} rows")
    for label, ev, ing in data:
        print(f"  {label:<26} event_ts={ev:%Y-%m-%d %H:%M}  ingested_at={ing:%Y-%m-%d %H:%M:%S}")


def main() -> int:
    client = client_from_env()
    show_real_ttls(client)
    client.command(f"DROP TABLE IF EXISTS {TABLE}")
    client.command(
        f"""CREATE TABLE {TABLE} (
                label String,
                event_ts DateTime64(3, 'UTC'),
                ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
            ) ENGINE = MergeTree ORDER BY label
            TTL toDateTime(ingested_at) + INTERVAL {TTL_MINUTES} MINUTE"""
    )
    try:
        print(f"\n{TABLE}: TTL toDateTime(ingested_at) + INTERVAL {TTL_MINUTES} MINUTE")
        now = datetime.now(UTC)
        replayed_day = datetime(2026, 10, 28, 12, tzinfo=UTC)
        client.insert(
            TABLE,
            [
                # event_ts is the replayed day in every row: only ingested_at decides.
                ["kept: ingested now", replayed_day, now],
                ["kept: ingested 2 min ago", replayed_day, now - timedelta(minutes=2)],
                ["gone: ingested 10 min ago", replayed_day, now - timedelta(minutes=10)],
                ["gone: ingested 1 h ago", replayed_day, now - timedelta(hours=1)],
            ],
            column_names=["label", "event_ts", "ingested_at"],
        )
        before = rows(client)
        print_rows("before OPTIMIZE FINAL", before)
        client.command(f"OPTIMIZE TABLE {TABLE} FINAL")
        after = rows(client)
        print_rows("after OPTIMIZE FINAL", after)
        removed = {r[0] for r in before} - {r[0] for r in after}
        expected = {r[0] for r in before if r[0].startswith("gone")}
        ok = removed == expected
        print(f"\nremoved {len(removed)} rows, expected {len(expected)}: {'OK' if ok else 'MISMATCH'}")
        return 0 if ok else 1
    finally:
        client.command(f"DROP TABLE IF EXISTS {TABLE}")
        print(f"dropped {TABLE}")


if __name__ == "__main__":
    sys.exit(main())
