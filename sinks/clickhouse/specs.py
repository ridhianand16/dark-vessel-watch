"""The sinks that exist: topic -> consumer group -> table -> row mapper.

Adding a sink is a mapper module, a migration for its table, an entry here and a
Compose service running `python -m sinks.clickhouse <name>`. Consumer group ids are
part of the deployed state: renaming one makes that sink re-read its topic from the
start (harmless, the tables are idempotent, but slow for ais.raw).
"""

from __future__ import annotations

from common.kafka_batch import BatchSettings
from sinks.clickhouse import alerts, raw
from sinks.clickhouse.sink import SinkSpec

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
            # Alerts are few and count against the 5 s p99 end-to-end budget, of which
            # the Flink job's 3 s checkpoints take most (ADR 0014): a lone alert waits
            # at most max_wait_s for company before it is inserted.
            settings=BatchSettings(max_rows=10_000, max_wait_s=0.2),
        ),
    )
}
