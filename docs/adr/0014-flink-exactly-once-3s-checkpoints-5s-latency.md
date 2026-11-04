# 0014. Flink enrichment: exactly-once with 3 s checkpoints, and a 5 s latency target

Status: accepted (2026-11-04)

## Context

The Flink enrichment job sits between `ais.raw` and everything downstream. It validates,
deduplicates and (later) zone-tags every message, writing `ais.enriched`, `ais.dlq` and
`ais.dedup_counts`. Reconciliation has to show, per run and partition, that each raw
message ended up in exactly one of those (ADR 0015). If the job could repeat or drop output
when a TaskManager dies, the equation would only hold on good days.

Flink can give end-to-end exactly-once with Kafka on both sides, but it has a cost. Output
becomes visible only when a checkpoint completes, so the checkpoint interval is added to
latency. The original target was p99 under 2 s from replayer send to alert written, at 10x.
That cannot coexist with transactional output committed every few seconds.

The laptop gives Docker a 3.9 GiB VM (OrbStack), and everything else is already in it.

## Decision

1. **Checkpoints every 3 s, mode EXACTLY_ONCE, heap state.** A checkpoint snapshots every
   operator's state together with the Kafka source offsets, by sending barriers through the
   stream. EXACTLY_ONCE aligns those barriers, so no record ends up in two snapshots, or in
   none. State is a few MB of Java maps (dedup keys for 2 minutes of event time, open count
   windows), so it lives on the heap (`HashMapStateBackend`) and is written to a shared Docker
   volume. Checkpoints are retained on cancel (3 kept), so a crashed job can be restored by hand.
2. **All three Kafka sinks are transactional (`DeliveryGuarantee.EXACTLY_ONCE`).** Each sink
   writes into a Kafka transaction, pre-commits it when a checkpoint starts and commits it
   when the checkpoint completes (two-phase commit). On failure the job restarts from the last
   completed checkpoint, aborts the open transactions and replays from the checkpoint's
   offsets. Readers use `isolation.level=read_committed`, so they never see the aborted part.
   Each sink has a fixed transactional id prefix (`dvw-enrichment-enriched`, `-dlq`,
   `-counts`). A restarted job finds and aborts its predecessor's transactions by that
   prefix, so the prefix must never change.
3. **`transaction.timeout.ms` = 10 minutes.** Flink's default of 1 hour is refused at
   startup, because Redpanda caps transactions at `transaction_max_timeout_ms` (15 min). Ten
   minutes is also the longest the job may stay down between a checkpoint and its recovery.
   After that, the broker aborts the pre-committed transaction and its output is lost. A
   TaskManager restart takes seconds, so this matters only if the cluster stays down.
4. **Latency target: p99 under 5 s at 10x replay.** This replaces 2 s. A record waits on
   average half a checkpoint interval (1.5 s), and at worst a full one, before it is
   committed. The p99 of that wait alone is about 2.97 s, plus the commit itself. That
   leaves about 2 s for the tracker, the alerts sink and ClickHouse. The figure gets tuned
   once there is latency measurement in the pipeline.
5. **Parallelism 4, maxParallelism 128, one TaskManager with 4 slots.** `maxParallelism`
   is the number of key groups, which is how keyed state is split up and moved when
   parallelism changes. It cannot change without discarding savepoints, so it is set
   explicitly from the start. The 6 `ais.raw` partitions spread unevenly over the 4 source
   subtasks (2, 2, 1, 1); `keyBy` evens it out by key group.
6. **Memory.** JobManager `process.size` 768m. TaskManager 1280m with
   `managed.fraction` 0.1, because heap state does not use managed memory (that is for
   RocksDB and batch sorting). This leaves about 410 MB of task heap. To fit, Redpanda drops
   to `--memory 512M` (container 768m), ClickHouse to 1.25 GB, and Console only runs on
   demand. Container limits equal Flink's process sizes, since Flink budgets heap,
   metaspace and JVM overhead inside them.
7. **Restarts and savepoints.** Restart strategy fixed-delay, 5 s, unlimited attempts;
   heartbeat timeout 20 s, so a dead TaskManager is noticed quickly. `make flink-savepoint`
   is stop-with-savepoint, not "take a savepoint and carry on": stopping lets the sinks commit
   everything up to the savepoint. `make flink-restart` always restores the newest savepoint.
   Restoring an older one after the job has moved on would write that stretch of output a
   second time.

## Alternatives

- **At-least-once sinks plus downstream dedup.** Output would be visible immediately (lower
  latency), and the tracker already deduplicates. But `ais.dedup_counts` must itself be
  exactly-once to balance the equation, and an at-least-once DLQ would double-count
  rejects. The equation would need a dedup step of its own on every reader.
- **1 s checkpoints.** Latency would drop by about 2 s, but there would be three times
  as many transactions and checkpoint files. Redpanda caps open transactional sessions at
  `max_transactions_per_coordinator` (10,000) and evicts the oldest. The Kafka connector for
  Flink 1.20 (3.4.0) opens a new transactional id per subtask per checkpoint, so 1 s would
  churn 12 ids per second. 3 s is a starting point, to be tuned with measurements.
- **Keep the 2 s target and turn exactly-once off.** That gives up the property the
  reconciliation job is there to prove.
- **RocksDB state backend.** It is for state larger than the heap. Ours is a few MB, and
  RocksDB would need the managed memory we are cutting to fit the laptop.

## Consequences

- Downstream consumers of the Flink topics must read with `read_committed`. The tracker
  does that when it moves to `ais.enriched`.
- End-to-end p99 is 5 s, not 2 s. The spec has been updated.
- Redpanda's transactional-id table sees about 4 new ids a second (3 sinks x 4 subtasks per
  3 s). Redpanda bounds this at 10,000 per coordinator by evicting the oldest. Flink never
  reuses an id from an earlier checkpoint, so eviction is harmless.
- One failure loses data: the job dies after a checkpoint completes but before its
  transactions commit (normally a gap of milliseconds), and then stays down for more than
  10 minutes. The records in those transactions come before the checkpoint's offsets, so they
  are not replayed. Transactions of a checkpoint that never completed are harmless. They are
  aborted and their records replayed. The Flink check would show the gap as "missing".
- The job adds 2 GB of container limits. Measured use is in docs/results.md.
