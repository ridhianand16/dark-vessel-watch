"""The sinks that exist: topic -> consumer group -> table -> row mapper.

Adding a sink is a mapper module, a migration for its table, an entry here and a
Compose service running `python -m sinks.clickhouse <name>`. Consumer group ids are
part of the deployed state: renaming one makes that sink re-read its topic from the
start (harmless, the tables are idempotent, but slow for ais.raw).
"""

from __future__ import annotations

from sinks.clickhouse import alerts, raw
from sinks.clickhouse.sink import Settings, SinkSpec

SPECS: dict[str, SinkSpec] = {
    s.name: s
    for s in (
        SinkSpec(
            name="raw",
            topic="ais.raw",
            group_id="dvw-raw-sink",
            table="dvw.ais_raw",
            columns=raw.COLUMNS,
            to_row=raw.to_row,
        ),
        SinkSpec(
            name="alerts",
            topic="ais.alerts",
            group_id="dvw-alerts-sink",
            table="dvw.alerts",
            columns=alerts.COLUMNS,
            to_row=alerts.to_row,
            # Alerts are few and count against the 2 s end-to-end budget: a lone alert
            # waits at most max_wait_s for company before it is inserted.
            settings=Settings(max_rows=10_000, max_wait_s=0.25),
        ),
    )
}
