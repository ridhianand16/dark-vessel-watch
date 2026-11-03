# 0004. Tracker: at-least-once, outputs acknowledged before state, idempotent sinks

Status: accepted (2026-11-02)

## Context

The Faust tracker (`stream/faust_app`) reads `ais.raw`, keeps a Kalman filter per MMSI
in a table, and writes one `ais.tracks` message per accepted fix. Three things change
on every record, and Kafka cannot commit them together without transactions:

1. the `ais.tracks` message,
2. the vessel's state (Faust writes table changes to a compacted changelog topic;
   the local RocksDB is only a cache of it),
3. the consumer offset on `ais.raw`.

The spec requires that killing a worker mid-run loses nothing and creates no
duplicate alerts. Downstream stores (ClickHouse, PostGIS) are outside Kafka, so they
cannot join a Kafka transaction in any case.

Two Faust behaviours shape the design (checked in faust-streaming 0.15.4):

- In at-least-once mode, `Consumer._commit_offsets` flushes the producer before it
  commits offsets. Outputs and changelog writes are therefore never behind the
  committed offset. They can be ahead of it, by up to one commit interval (2.8 s).
- An exception in an agent body is logged and the agent restarts, but the events it
  was processing are still acked (`take_events` acks in a `finally`). Their offsets
  are then committed, and the records are silently dropped.

## Decision

1. **At-least-once** (`processing_guarantee="at_least_once"`, the Faust default).
2. **Outputs before state.** Records are taken in batches (`take_events`, up to 1,000
   records or 0.1 s). For each batch the tracker computes all track messages and new
   states in memory, sends the tracks and waits for the broker's acknowledgements,
   and only then writes the states to the table. After a crash, a vessel's state is
   therefore either from before the batch or from after its tracks were stored.
   It is never ahead of `ais.tracks`.
3. **The state records the last applied offset.** A record whose offset is at or
   below the vessel's `last_offset` was already applied, so it is skipped and
   counted as `redelivered`. All of a vessel's records are in one partition, since
   the key is the MMSI, so offsets order them.
4. **Deterministic output.** A track message depends only on the stored state and the
   input record: no wall-clock fields, no random IDs. A record that is reprocessed
   from the old state produces byte-identical bytes.
5. **Sinks dedup on `(src_partition, src_offset)`.** Every track message carries the
   `ais.raw` coordinates of its fix. The ClickHouse tracks table will use them as its
   ReplacingMergeTree key, as `ais_raw` does (ADR 0003).
6. **Fail-stop.** Any unexpected exception while processing a batch exits the
   process at once (`os._exit(70)`). Nothing is acked or committed, so Compose
   restarts the worker from the last commit. On a clean shutdown the batch in flight
   is shielded from cancellation and finishes before Faust acks it.

What happens after a crash, by when it struck:

| Crash during | State | Offsets | On restart |
|---|---|---|---|
| compute or send | old | old | batch reprocessed, identical tracks resent; sinks dedup |
| state writes | some vessels new | old | new vessels: records skipped as `redelivered`; others: as above |
| after writes, before commit | new | old | all records skipped as `redelivered` |

## Alternatives

- **Faust exactly-once** (`processing_guarantee="exactly_once"`). Kafka transactions
  make the outputs, the changelog writes and the offsets atomic. Rejected for now:
  - It covers only Kafka. The ClickHouse and PostGIS sinks are separate consumers
    and need to be idempotent anyway, so in practice it removes only duplicate
    messages on `ais.tracks`, which those sinks already absorb.
  - Faust opens one transactional producer per assigned source partition (6 here),
    and every consumer of the outputs must use `read_committed`. Output becomes
    visible only when a transaction commits, which adds up to one commit interval
    of latency against the 2 s p99 budget.
  - Its rebalance and transaction-fencing paths are the least-tested parts of
    faust-streaming, and on failure they tend to stall rather than fail loudly.
  Reconsider if a consumer of `ais.tracks` turns out to need exactly-once semantics
  and cannot dedup.
- **Plain per-record loop** (`async for event in stream`, table write and send for
  each record). Simpler, but the changelog write and the track message are flushed
  independently. A hard kill between them can store a state whose track message was
  never written; the replayed record is then skipped as already applied, and that
  track is lost. Waiting for an acknowledgement on every record would close the gap
  but limit throughput to one broker round trip per record.
- **No redelivery check.** Replayed records would reach the filter again: earlier
  event times would be counted as `out_of_order`, which mixes up two different
  problems, and equal timestamps would update the filter twice with the same fix.

## Consequences

- **`ais.tracks` can contain duplicates** after a crash, always byte-identical and
  with the same `(src_partition, src_offset)`. A sink that does not dedup on that pair
  is a bug.
- **`take_events` breaks a Faust assumption.** The table picks its changelog
  partition and RocksDB partition from the "current event", which inside a batch is
  the last one. `app.as_current_event` sets it per read and write through
  `faust.streams._current_event`, a private API. faust-streaming is pinned to
  `<0.16` and `test_app.py` checks the behaviour.
- **Counters are in-process** (`tracks=... duplicate=... redelivered=...` every 30 s
  in the log) and reset on restart. Prometheus metrics come with the observability work.
- **Redpanda Console shows lag on `dvw-tracker-vessel_state-changelog`** equal to
  its size. Faust reads changelogs by direct assignment during recovery and never
  commits offsets for them, so this lag is expected and not a backlog.
- **One worker for now.** Batches can span partitions, so a rebalance while several
  workers run needs testing before scaling out (the k8s deployment).
- **Exact duplicate fixes are skipped** (same `event_ts` and position as the last
  accepted fix, about 40% of DMA rows). This is a filter-correctness rule, not dedup
  of the stream: the Flink job will remove them upstream, and the check stays as a
  cheap guard.
