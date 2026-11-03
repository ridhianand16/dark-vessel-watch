# 0015. Dedup key, and counting what dedup drops so raw = enriched + dlq + duplicates

Status: accepted (2026-11-04)

## Context

About 40% of DMA rows are the same position report heard by more than one base station.
The Flink enrichment job should forward one copy. Reconciliation has worked so far
because every raw message lands somewhere countable (`produced = stored`, ADR 0003). Once
Flink drops duplicates, a raw message can legitimately end up nowhere. Without an exact
count of what was dropped, "dropped as a duplicate" and "lost" look the same.

Two copies are not always the same report. An MMSI is sometimes shared by two ships, and
that is the evidence MMSI_CLASH needs, so dedup must not merge them.

## Decision

1. **Duplicate key: (run_id, MMSI, event_ts, lat, lon), coordinates rounded to 5 decimal
   places (about 1 m).** A repeat from another receiver has the same second and position,
   so it is a duplicate. A second ship using the MMSI in the same second has a different
   position, so it survives. The stream is keyed by MMSI, and the run_id is part of the
   key inside each vessel's state. Replaying the same day again is a new run, and its
   records are not duplicates of the previous run's: each run must balance on its own.
2. **A key is remembered for 2 minutes of event time**, measured by two clocks:
   - The vessel's own latest event time in that run, checked on every record. The replayer
     sends each vessel in event-time order (ADR 0002), so this is deterministic. It also
     keeps state bounded when the watermark cannot move. For example, when the same day is
     replayed behind a watermark that an earlier run already pushed to the afternoon,
     timers in the past never fire.
   - An event-time timer at the vessel's latest time + 2 minutes. When the watermark
     passes it, the run is quiet for that vessel and its keys are dropped.

   Not state TTL: TTL counts wall-clock time, and at 10x a 2-minute TTL covers 20 minutes
   of ship time.
3. **Dropped duplicates are counted exactly-once, in Kafka, not in metrics.** The dedup
   operator sends each duplicate to a side output, a second typed output of the same
   operator. From there they are keyed by (run_id, ais.raw partition) and counted in 10 s
   tumbling event-time windows. The counts are written through the same transactional sink
   as everything else, to `ais.dedup_counts` (ADR 0016). A Flink metric counter would not
   do: metrics reset on restart and count again during recovery. The job does keep
   `duplicates` and `dlq` counters, but only for dashboards; the check never reads them.
4. **Count windows fire on the watermark, and also on a 5 s wall-clock flush.** At the end
   of a replay every partition goes idle, and the watermark stops short of the last
   windows. With event time alone, their counts would sit in state until the next run, and
   the check would come up short. A custom trigger fires and purges a window when the
   watermark passes its end, or when an aligned processing-time timer fires, whichever
   comes first. One window can therefore produce several count records (an early flush,
   then a late duplicate). Readers sum them; nothing reads a single record as "the count
   for that window".
5. **No duplicate can fall out of the count.** Windows allow 3 days of lateness, which
   covers every valid event time (the AIS day plus or minus 1 day). Anything later still
   goes to a late side output and is written as a count of 1.
6. **The equation, per run and ais.raw partition, over the run's offset range:**
   `raw = enriched + dlq + sum(dedup_counts.count)`. Every enriched and DLQ record carries
   the `src_partition` and `src_offset` it came from, so the check also proves no raw offset
   appears twice across `ais.enriched` and `ais.dlq` (`make flink-check RUN=<id>`). All three
   topics are read with `read_committed`.

## Alternatives

- **Key without run_id.** If a run is replayed within 2 minutes of event time of an earlier
  one, its first records would be dropped as copies of the earlier run's. The equation
  would still balance, but `ais.enriched` would be missing that run's records.
- **keyBy (run_id, MMSI).** That scopes state per run as well, but it changes the key every
  later operator (zone tagging, buffering) will want, which is MMSI.
- **Duplicates to a topic instead of counts.** That gives per-offset proof instead of a
  count, but it writes about 40% of the input volume again, for a check a count settles.
- **Default event-time trigger.** The last windows of every run would stay open until the
  next run's data arrived.
- **Count in a KeyedProcessFunction flushed on wall-clock time only.** It is simpler, but
  the request was for event-time windows, and the trigger keeps them while fixing the end
  of run.

## Consequences

- `ais.dedup_counts` has several records per (run, partition, window). The only valid
  aggregate is a sum.
- A run replayed behind another run's watermark is deduplicated on its own vessel clocks.
  If the watermark later moves past it (a third, concurrent run), that run's keys can be
  dropped early, and a few duplicates go forward to `ais.enriched`. The equation still
  balances, because they are counted as enriched. The tracker's own duplicate check stays
  as a safety net.
- Count windows keep a cleanup timer for 3 days of event time after they close. That is
  about 52,000 small timers for a full-day run, released once a later day pushes the
  watermark past them.
