# 0003. Raw sink: Python consumer, commit after insert, ReplacingMergeTree

Status: accepted (2026-11-02)

## Context

Every message on `ais.raw` must end up in ClickHouse, and the zero-loss check
(`produced = stored` per partition offset range) must hold even if the sink crashes,
is killed, or re-reads the topic. ClickHouse has no transactions that span a Kafka
offset commit, so the write and the commit cannot be made atomic. One of them has to
happen first, and the table has to absorb the consequences.

Messages are not guaranteed to be valid. The replayer sends every CSV line (ADR 0002),
and anyone with access can produce to the topic. The raw table is the record of what
arrived, so a value that does not decode must still be stored.

## Decision

1. **A small Python consumer** (`sinks/clickhouse_raw`, confluent-kafka and
   clickhouse-connect), group `dvw-raw-sink`, `enable.auto.commit=false`,
   `auto.offset.reset=earliest`, `isolation.level=read_committed`.
2. **Commit after insert.** Collect up to 50,000 records or 1 s after the first one,
   insert them in one call, then commit the next offset per partition synchronously.
   That is at-least-once: a crash between insert and commit re-delivers the batch.
3. **The table makes redelivery harmless.** `dvw.ais_raw` is a `ReplacingMergeTree`
   with `ORDER BY (kafka_partition, kafka_offset)`. A record's Kafka coordinates
   identify it, so a second copy collapses with the first.
4. **Failures.** A failed insert is retried with exponential backoff (6 attempts, about
   15 s in total) without committing. After that the process exits 1, and Compose's
   `restart: unless-stopped` restarts it from the last commit. SIGTERM finishes and
   commits the in-flight batch, then exits 0.
5. **Never drop.** A value that is not UTF-8 or not JSON, has the wrong
   `schema_version`, or has a field that does not fit its column still becomes a row,
   with the original bytes in `payload`, the `run_id` if it can be found, and a reason
   code in `decode_error`. Judging values (lat out of range, bad MMSI) is the Flink
   job's work, not this sink's.

## Alternatives

- **ClickHouse Kafka table engine with a materialized view.** No extra service, and it
  is the usual way to do this. But the engine commits offsets on its own schedule, it
  handles messages that fail to parse by either skipping them
  (`kafka_skip_broken_messages`) or stopping, which conflicts with "never drop", and
  it is harder to observe and test from outside. Reconsider once the sink is boring.
- **Kafka Connect with a ClickHouse connector.** Mature, but it adds a JVM service with
  around 1 GB of heap on an 8 GB laptop, and the connector's exactly-once mode relies on
  a KeeperMap state table. That is a lot of moving parts for one table.
- **Exactly-once through transactions.** ClickHouse cannot take part in a Kafka
  transaction. Storing offsets in ClickHouse in the same insert would work, but then the
  sink has to manage its own offsets instead of using the consumer group. Since the
  table is idempotent anyway, at-least-once gives the same result with less code.

## Consequences

- **Dedup happens at merge time.** Until a background merge combines the parts, a
  redelivered batch is stored twice. Reads that need exact counts (reconciliation)
  use `FINAL`. Dashboards that can tolerate brief overcounting don't need it.
- **The partition key must use only fields that are identical in every delivery** of
  the same record: `toDate(ifNull(event_ts, kafka_ts))`. ReplacingMergeTree never
  dedupes across partitions, so a key on `ingested_at` would let duplicates escape.
  The sink treats event timestamps outside 2000..2100 as bad fields (null, which falls
  back to `kafka_ts`). Otherwise junk dates could exceed the 100-partitions-per-insert
  limit and make a batch fail on every retry.
- **Offsets are the identity of a row.** Deleting and re-creating `ais.raw` without
  also truncating `dvw.ais_raw` would reuse offsets, and new records would replace old
  ones. `make nuke` drops both together. Reconciliation catches this case anyway:
  rows in a run's range must carry that run's `run_id`.
- **More state to watch:** consumer lag for `dvw-raw-sink`, the sink's restart count,
  rows with `decode_error != ''`, and the parts count (many small parts mean batches
  are too small). These belong on the dashboard.
- **A poison batch can stall the sink.** If an insert fails deterministically (not
  because of a value, which is mapped, but because of something like a schema change),
  the sink restarts in a loop at the same offset. The loop is visible from the restart
  count and lag, and nothing is lost, but nothing moves either until it is fixed.
