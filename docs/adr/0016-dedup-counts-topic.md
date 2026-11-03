# 0016. New topic ais.dedup_counts

Status: accepted (2026-11-04)

## Context

The Flink enrichment job drops duplicate reports and has to account for each one (ADR
0015). The counts must be durable, exactly-once, and readable by the reconciliation job
without a database. The five existing topics each carry one kind of record, and none of
them fits.

## Decision

A sixth topic, `ais.dedup_counts`, created by `init-topics` like the others: 6 partitions,
replication 1, and the partition count asserted after creation.

Record value (JSON, UTF-8, schema_version 1):

| Field | Type | Meaning |
|---|---|---|
| `schema_version` | int | 1 |
| `run_id` | string | run of the dropped duplicates |
| `src_partition` | int | their `ais.raw` partition |
| `window_start`, `window_end` | int | 10 s event-time window, epoch ms UTC, end exclusive |
| `count` | int | duplicates in this firing of the window |

Key: `run_id`, so a run's counts sit in one partition and the producer's default murmur2
partitioner places them (ADR 0001). Written only by the Flink job, through a transactional
sink. Readers use `read_committed` and sum `count`.

## Alternatives

- **Counts as records in `ais.dlq`** (reason `duplicate`). The DLQ means "this record was
  rejected", one record per raw message. A count record would break both meanings, and
  every DLQ reader would need to know about it.
- **A ClickHouse table written by Flink.** That needs a JDBC sink with its own idempotency
  scheme, and the check would read two systems. A topic uses the transactions the job
  already has.
- **One partition.** It is enough for the volume, but every topic in the stack has 6, and
  `init-topics` asserts that. An exception would be a special case for no gain.

## Consequences

- One more topic to keep: tiny (one record per run, partition and 10 s window that saw
  duplicates), with default retention.
- A ClickHouse table for it can come later, if the counts are wanted in dashboards.
