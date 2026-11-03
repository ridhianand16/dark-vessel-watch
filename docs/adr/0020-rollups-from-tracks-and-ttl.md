# 0020. Rollups from tracks with duplicate-insensitive aggregates; TTL on ingested_at

Status: accepted (2026-11-04)

## Context

An investigator wants one vessel's day at several resolutions: every report, one
position per minute, one per hour. `ais_raw` keeps every record as it arrived,
including non-ship stations, invalid rows and the duplicates the Flink job drops. The
tracker's `ais.tracks` has one message per accepted fix, ships only, Kalman-filtered,
and it was not stored in ClickHouse yet.

ClickHouse materialised views are insert triggers. A view on a ReplacingMergeTree table
sees every inserted block, including a batch the sink re-inserts after a crash and a
track the tracker resends (both at-least-once, ADR 0003 and 0004). The table's merge
removes the copy later; anything the view already aggregated from the copy stays.

Retention has a twist: the pipeline replays a past day (2026-10-28), so every
`event_ts` is already days old when it arrives.

Everything runs in a 1.25 GB ClickHouse that idles at 450 to 800 MiB.

## Decision

1. **`dvw.tracks`** stores `ais.tracks` (migration 0003): a ReplacingMergeTree ordered by
   `(mmsi, event_ts, src_partition, src_offset)`, the fix's identity, partitioned by
   `toDate(event_ts)`, which is part of the identity, so copies share a partition. A
   record whose identity cannot be decoded is stored with `mmsi = 0` and its own Kafka
   coordinates as the key.
2. **Rollups read `dvw.tracks`**, not `ais_raw` (migration 0004): `ais_1min` and
   `ais_1hour` are AggregatingMergeTree tables, each with its own view on `dvw.tracks`.
   The 1-hour view does not read the 1-minute table: two independent GROUP BYs, nothing
   to keep in step.
3. **Only aggregates a duplicate cannot change.** The last fix is
   `argMax((lat, lon, sog, cog), (event_ts, src_offset))`, `min`/`max` speed, `max`
   innovation, and `n_fixes = uniqExact((src_partition, src_offset))`. No `count()`,
   `sum()` or `avg()`. Adding a fix twice gives the same row, so redeliveries, resends
   and repeated backfills cannot inflate the rollups.
4. **Key `(run_id, mmsi, bucket)`.** A second replay of the same day has the same event
   times and new offsets; without `run_id` its fixes would be counted into the first
   run's buckets.
5. **One `argMax` over a tuple**, not one per field. Measured on one run (413k
   fixes over the first hour of the day, 92k minute buckets): merging six parts took 147 MiB instead of 252 MiB, and
   the 1-minute GROUP BY ran 3.6 times faster (625 ms instead of 2278 ms). With four
   states, merges during the first catch-up hit ClickHouse's memory limit.
6. **Read through `ais_1min_v` and `ais_1hour_v`**, which apply the `-Merge`
   combinators. `make backfill-rollups` fills the rollups from tracks stored before the
   views existed, with the views' own SELECT, one run and partition at a time.
7. **TTL on `ingested_at`** (migration 0005): `ais_raw` and `tracks` 7 days, `ais_1min`
   30 days (on the latest insert into the bucket), `ais_1hour` and `alerts` kept. The
   TTL is set without rewriting existing parts (`materialize_ttl_after_modify = 0`).
8. **Tracks and alerts sinks share a process** (`tracks-alerts-sink`, 320m): one thread
   and consumer group per sink. If either fails, both stop after committing what they
   wrote and the process exits, so fail-stop holds.

## Alternatives

- **Roll up from `ais_raw`.** Every record, but it includes base stations, invalid
  positions and repeats that the tracker removes for good reasons, and the positions
  are unfiltered. Raw history stays available in `ais_raw` for that.
- **Chain 1-hour from 1-minute.** Cheaper per insert in theory, but the 1-hour view
  would have to read partial states with `-MergeState`, and a fix to one view would
  need the other redone. Both views are small next to the tracks insert itself.
- **Accept double counting** after a crash, since the product is alerts, not traffic
  statistics. It costs nothing to avoid with these aggregates, and an exact
  `n_fixes` is what lets the rollups be checked against `count() FINAL`.
- **`groupBitmap` for `n_fixes`.** Exact and duplicate-proof too, with 6 times less
  memory for the 1-hour GROUP BY and 2.3 times less disk, but merges cost the same as
  with `uniqExact` (145 against 147 MiB) and the value would have to pack partition and
  offset into one UInt64. Worth it if the 1-hour table grows large.
- **TTL on `event_ts`.** Deletes the replayed day on arrival, and in live use would drop
  late or resent records on insert.
- **Let `MODIFY TTL` rewrite existing parts.** On a 690 MiB `ais_raw` that mutation ran
  out of memory and failed the migration once before the retry succeeded.
- **A separate tracks-sink container** (256m). Simpler, 128 MB more.

## Consequences

- **Exact checks:** for any run, `sum(n_fixes)` in `ais_1min_v` and in `ais_1hour_v`
  equals `count() FINAL` in `dvw.tracks` (all 26 runs on the laptop matched after the
  backfill, and a 900,000-row replay gave 502,264 at all three). Re-inserting all
  2.8 million tracks by resetting the sink's consumer group left every run's counts
  and every rollup row's values unchanged.
- **Reading a whole rollup is expensive:** the `-Merge` view materialises a
  `uniqExact` set per bucket. A query over every run's minutes hit the memory limit;
  filter by `run_id` and `mmsi` first, which the key makes cheap.
- **The last fix agrees across resolutions:** the tuple ordering breaks `event_ts` ties
  by `src_offset`, so the last row of a minute in `tracks`, the minute's rollup and
  the hour's rollup are the same fix.
- **Rows outlive their TTL** until a merge runs (`merge_with_ttl_timeout`, 4 hours),
  and parts that existed before migration 0005 only expire after a merge or a manual
  `MATERIALIZE TTL`.
- **Reconciliation of old runs** is limited to 7 days: after that `ais_raw` no longer
  has the rows to count.
- **One process for two sinks** means a fault in one restarts the other, and a large
  tracks batch can hold the GIL for a few milliseconds while an alert waits.
