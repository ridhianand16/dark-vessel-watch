"""Consume one topic and insert every record into one ClickHouse table.

    uv run python -m sinks.clickhouse raw      # ais.raw -> dvw.ais_raw (Compose: raw-sink)
    uv run python -m sinks.clickhouse alerts   # ais.alerts -> dvw.alerts (Compose: alerts-sink)

What differs between instances is a SinkSpec (specs.py): topic, consumer group, table,
columns and the row mapper. Batching, retries and offset commits are the shared loop
in common/kafka_batch.py.

Delivery: at-least-once into an idempotent table. Offsets are committed only after the
batch insert has succeeded. A crash between insert and commit re-delivers that batch;
the table's ReplacingMergeTree key collapses the copies (raw: Kafka coordinates,
alerts: alert_id). Exit codes: 0 after a clean shutdown, 1 when inserts keep failing
or the table is missing, 2 for an unknown sink name (argparse).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer

from common.kafka_batch import BatchSettings, BatchSink

RowMapper = Callable[[bytes | None, int, int, int], list[object]]
"""(value, partition, offset, kafka_ts_ms) -> the row's values in SinkSpec.columns order.
Must never raise: a value it cannot use still becomes a row, with a decode_error."""


@dataclass(frozen=True)
class SinkSpec:
    """One topic into one table: everything that differs between sink instances."""

    name: str
    topic: str
    group_id: str
    table: str
    columns: tuple[str, ...]
    to_row: RowMapper
    settings: BatchSettings = BatchSettings()

    def __post_init__(self) -> None:
        if "decode_error" not in self.columns:
            raise ValueError(f"{self.name}: columns must include decode_error")

    @property
    def decode_error_index(self) -> int:
        return self.columns.index("decode_error")


class ClickHouseSink(BatchSink[list[object]]):
    def __init__(self, spec: SinkSpec, consumer: Consumer, client: Client) -> None:
        super().__init__(spec.name, spec.topic, consumer, spec.settings)
        self.spec = spec
        self.client = client

    def to_row(self, value: bytes | None, partition: int, offset: int, kafka_ts_ms: int) -> list[object]:
        return self.spec.to_row(value, partition, offset, kafka_ts_ms)

    def write(self, rows: list[list[object]]) -> dict[str, int]:
        self.client.insert(self.spec.table, rows, column_names=list(self.spec.columns), settings={"async_insert": 0})
        i = self.spec.decode_error_index
        return {"decode_errors": sum(1 for r in rows if r[i])}
