# 0012. vessels_current: upsert per MMSI that only moves forward in event time

Status: accepted (2026-11-03)

## Context

The map and the DARK_GAP zone check need each vessel's latest position as a row they
can query spatially. That means one row per MMSI in PostGIS, kept current from
`ais.tracks`.

The ClickHouse sinks are idempotent because their tables collapse copies after the
fact (ReplacingMergeTree, ADR 0003, ADR 0006). An upsert has nothing to collapse: a
second write replaces the first. So the same track message can arrive more than once
(tracker at-least-once, ADR 0004; a sink crash between write and commit; a consumer
group reset; a second replay of the same day), and track messages can arrive older
than what is stored (an earlier run read after a later one). None of that may move a
row backwards or make the table depend on how often or in what order it was fed.

## Decision

1. **The guard is the event time, strictly.** `INSERT ... ON CONFLICT (mmsi) DO UPDATE
   ... WHERE excluded.event_ts > vessels_current.event_ts`. A row changes only for a
   strictly later fix. Re-delivered, reprocessed or older messages write nothing:
   `updated_at` stays put and no new row version is created.
2. **One row per MMSI per statement.** Postgres rejects an `ON CONFLICT DO UPDATE` that
   touches one key twice in a statement. Each batch is first reduced to the latest
   `event_ts` per MMSI. On a tie the first message received wins, the same rule the
   strict `>` applies across batches, so the stored row does not depend on where a
   batch boundary fell. Rows are sent ordered by MMSI.
3. **One statement, one transaction, then the offsets.** The batch goes as parallel
   arrays through `unnest()`, inside `conn.transaction()`. Offsets are committed only
   after COMMIT returns. A crash in between re-delivers the batch, and point 1 makes
   that a no-op.
4. **Same delivery loop as the ClickHouse sinks.** Batching, retries, commit ordering
   and SIGTERM handling moved from the ClickHouse sink to `common/kafka_batch.py`
   (`BatchSink`). The vessels sink supplies only `to_row()` and `write()`.
5. **An unusable track message is skipped, not stored.** There is no column to put a
   decode error in, and retrying would fail the same way forever. The sink logs it
   with its coordinates, counts it as `skipped` in the batch log line, and commits its
   offset. The tracker writes these messages, so a skip means a tracker bug, and the
   next good fix for that vessel corrects the row.

## Alternatives

- **`>=` as the guard.** The last copy of an equal fix would win, but every
  reprocessed message would rewrite its row and bump `updated_at`, so a reset consumer
  group would visibly change the table. It also creates a dead tuple per message for
  vacuum to clean up.
- **Guard on `(event_ts, kafka_offset)`.** That gives a total order, so a later run
  with identical fixes would take over the row and its `run_id`. But offsets reset if
  the topic is recreated, which would freeze rows that have higher stored offsets.
  Event time is the only order that means something to a reader.
- **`executemany` of single-row upserts.** No in-batch dedup needed, but one round trip
  per row (or pipeline mode), and a batch of 10,000 tracks touches only about 1,700
  vessels.
- **Keep the table in ClickHouse** (`ReplacingMergeTree` by MMSI, version `event_ts`).
  Correct after `FINAL`, but the zones live in PostGIS, and the spatial joins against
  them are why this table exists.
- **Faust writes PostGIS directly.** The tracker would block on the database, and a
  crash would mix DB writes into its outputs-before-state ordering (ADR 0004).

## Consequences

- **`run_id` is the run whose fix is stored, not the latest run.** Replaying the same
  day again leaves rows that already have that event time untouched. The row and its
  `run_id` move to the new run only where the new run reaches later fixes.
- **Replaying an earlier day after a later one changes nothing.** To start a table
  over (e.g. for a demo), truncate it and reset the group.
- **Verifying a run needs an empty table.** A table filled by earlier runs holds
  vessels the new run never sees, and vessels whose stored fix is later.
- **One more container** (192 MB limit), one more consumer group (`dvw-vessels-sink`).
  The sink image is shared by all three sinks (`sinks/Dockerfile`, `dvw-sinks`).
