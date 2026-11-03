# 0006. Alerts sink: the raw sink, driven by a spec, into a table keyed by alert_id

Status: accepted (2026-11-03); batch window amended by ADR 0019

## Context

The tracker writes DARK_GAP and TELEPORT alerts to `ais.alerts` (ADR 0005). They need
a ClickHouse table, and the delivery problem is the same as for `ais.raw` (ADR 0003):
the insert and the offset commit cannot be atomic, and the tracker itself is
at-least-once (ADR 0004), so the topic can hold the same alert more than once.

The raw sink already solves the delivery part, and it is tested. A copy of it would
duplicate that solution and leave two versions to keep in step. The tracks table
(later) has the same shape again.

## Decision

1. **One sink, many specs.** `sinks/clickhouse_raw` becomes `sinks/clickhouse`. What
   differs between instances is a `SinkSpec`: topic, consumer group, table, columns,
   row mapper and batch settings (`specs.py`). The delivery loop (collect, insert with
   retries, commit after insert, fail-stop) is shared. `python -m sinks.clickhouse
   raw|alerts` picks one; Compose runs `raw-sink` and `alerts-sink` from the same image.
2. **`dvw.alerts` is a `ReplacingMergeTree` ordered by `alert_id`** (migration 0002).
   `alert_id` is a UUIDv5 of (type, mmsi, run_id, trigger_ts), so a resent alert is
   the same row. Copies are byte-identical, so it does not matter which one survives,
   and there is no version column.
3. **No partition key.** ReplacingMergeTree dedupes only within a partition. Partitioning
   by alert time would be safe today, but nothing else in the row is guaranteed equal
   across copies, and alerts are a few thousand a day. A single partition cannot let a
   duplicate escape.
4. **Never drop, as the raw sink.** A record that does not decode is stored with
   `alert_id = invalid:<partition>:<offset>` and a `decode_error`. That id is unique per
   Kafka record, so the row cannot merge into a real alert, and its own redelivery
   still dedups. `alert_id` must be a canonical lowercase UUID: two spellings of one
   id would be two rows.
5. **Short batches.** The alerts sink inserts after at most 0.25 s, not 1 s like the
   raw sink. A lone alert would otherwise spend half of the 2 s end-to-end budget
   waiting for company.

## Alternatives

- **Copy the raw sink.** Fastest today, two delivery loops to keep correct tomorrow.
- **ClickHouse Kafka engine for alerts only.** The volume would suit it, but its
  offset handling and parse-error behaviour are the reasons ADR 0003 rejected it, and
  a second ingestion style doubles what has to be monitored.
- **`ORDER BY (type, mmsi, alert_id)`** for faster lookups by vessel. Same dedup, since
  the id determines both, but undecodable rows have null `mmsi`, which needs
  `allow_nullable_key`. At this volume a full scan is milliseconds. Revisit with a
  projection if the dashboard needs it.
- **Faust exactly-once on the tracker**, so the topic never holds a duplicate. Covered
  in ADR 0004: it would not remove the need for an idempotent sink.

## Consequences

- **Exact counts use `FINAL`**, as on `ais_raw`. Measured: after a simulated tracker
  resend of 6 alerts, `ais.alerts` held 90 messages, the table 90 rows before merging,
  and 84 with `FINAL`, one per `alert_id`.
- **Group ids are deployed state.** `dvw-raw-sink` was kept through the rename, and the
  raw sink resumed at its committed offsets. Renaming a group re-reads its topic.
- **Adding a sink** is a mapper module, a migration, a spec entry and a Compose service.
- **One more container** (192 MB limit), and one more consumer lag to watch.
