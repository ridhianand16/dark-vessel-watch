# 0019. The tracker reads ais.enriched; DARK_GAP's zone condition reads Flink's tags

Status: accepted (2026-11-04)

## Context

Until now the Faust tracker read `ais.raw` and did its own filtering: it skipped
invalid records and non-ship stations, and ignored exact repeats of the last report.
Its DARK_GAP rule had a zone predicate that returned true everywhere, so every
qualifying silence fired, in port and in open water alike.

The Flink enrichment job now writes `ais.enriched` (ADR 0014 to 0018): only valid
records, repeats dropped, each record carrying its `ais.raw` partition and offset, a
`station_class`, and zone tags (`in_port`, `dist_eez_boundary_m`, `dist_mpa_m`,
`mpa_ids`, `eez_id`, `zones_version`). It writes in Kafka transactions that commit with
each 3 s checkpoint.

Three things have to stay true when the tracker moves over:

- **No record is skipped by mistake.** The tracker's per-vessel bookmark (ADR 0004)
  is "the last offset applied"; a record at or behind it is treated as already done.
  Flink drops about 40% of raw records, so `ais.enriched` offsets run well behind
  `ais.raw` offsets. Compared against bookmarks stored as raw offsets, almost every
  enriched record would look old.
- **No record of an aborted Flink transaction is seen.** After a TaskManager failure,
  the records written since the last checkpoint are aborted and written again.
  A `read_uncommitted` reader sees both copies.
- **Tracks do not change** for the records both inputs share, so everything
  downstream (the sinks' dedup keys, `vessels_current`) keeps working.

## Decision

1. **Input topic from config.** `TRACKER_INPUT_TOPIC`, default `ais.enriched`. `ais.raw`
   is still accepted, to compare tracks before and after the switch and to run without
   Flink.
2. **`read_committed` for the consumer only.** Faust reads committed only when its
   `processing_guarantee` is `exactly_once`, which also makes its producer
   transactional. The tracker stays at-least-once with outputs before state (ADR 0004).
   So the tracker registers a small Faust transport (`stream/faust_app/read_committed.py`)
   that sets aiokafka's isolation level, and points `broker_consumer` at it. The
   producer keeps the stock transport. Two tests guard the private aiokafka attribute.
3. **The bookmark stays a raw offset.** On `ais.enriched` the tracker uses the record's
   `src_offset` for the bookmark and puts `src_partition`/`src_offset` on its tracks, as
   before. This is safe because per-vessel order survives Flink: one MMSI comes from one
   raw partition, Flink keys by MMSI and keeps per-key order, and its zone buffer
   flushes in order. Flink's sinks use the same murmur2 partitioner as the replayer
   (ADR 0001), so a vessel's enriched partition is its raw partition. The gap scan
   relies on that (vessel state is co-partitioned with the input). A record that
   breaks it stops the tracker (exit 70) instead of being tracked on a wrong partition.
4. **The tracker's own checks stay, as a safety net with counters.** Whatever they
   catch on `ais.enriched` is something Flink should have removed: `leak_invalid`
   (any skip except `not_a_ship`), `leak_station_class` (Flink's `station_class` and the
   tracker's MMSI rule disagree; the record is skipped), `leak_duplicate` (the exact
   repeat check fired). The tracker logs their sum as `flink_leaks` every 30 s.
   Non-ship stations are valid in `ais.enriched` and are skipped as before, not counted
   as leaks.
5. **DARK_GAP's zone condition:** the vessel's last accepted fix is within
   `DARK_GAP_ZONE_NM` (10 nm) of an EEZ boundary or a protected area, by Flink's
   geodesic distances, and not in port. The tags of the last accepted fix are kept in
   the vessel state (state version 3) and copied into the alert's `details.zone`. A
   record without tags (from `ais.raw`, or a distance Flink could not compute) is near
   nothing, so no DARK_GAP. The tracker never queries PostGIS.
6. **Alerts sink batch window 200 ms** (was 250 ms, ADR 0006 point 5). The Flink
   checkpoint takes most of the 5 s budget now.

Switching a running tracker: stop it, seek its group to the end of `ais.enriched`
(`rpk group seek dvw-tracker --to end --topics ais.enriched --allow-new-topics`), start
it. Without the seek, a group with no commits on `ais.enriched` starts at the earliest
offset and re-tracks every earlier run. Records of a run already processed from `ais.raw`
are behind the bookmark either way.

## Alternatives

- **Faust `exactly_once`.** Gets `read_committed` with a supported setting, but makes
  the tracker's output transactional too, with one transaction per changelog/output
  commit. That is a different delivery design than ADR 0004, with its own latency and
  failure modes, for a problem that is only on the read side.
- **Compare enriched offsets, and reset bookmarks at the switch.** Works for the switch
  itself, but every later comparison mixes two offset spaces if the tracker is ever
  pointed back at `ais.raw`, and a raw offset is what the tracks already carry as their
  identity.
- **The tracker queries PostGIS (or keeps its own zone index) at gap time.** Duplicates
  the Flink job's zone work and its correctness checks (ADR 0013, 0017, 0018), adds a
  database round trip to the hot path, and could tag with a different zone package than
  the records were tagged with.
- **Zone of the last fix heard, not the last accepted one.** "Last heard" is when the
  silence is measured from, but a heard fix may be a rejected TELEPORT outlier. Its
  position is the one the tracker judged implausible, so it should not decide whether a
  gap counts. The alert's position is the last accepted fix too.
- **Keep the always-true predicate when reading `ais.raw`.** Would keep DARK_GAP alive
  without Flink, but under a different rule than the one documented. No tags, no alert
  is easier to reason about.

## Consequences

- Same tracks: a 10,000-row replay read from `ais.raw` and again from `ais.enriched`
  gives identical tracks for 1,278 of 1,279 vessels, with no leak counted. The one
  difference is by design: Flink's duplicate key rounds positions to 5 decimals
  (ADR 0015), so a second report in the same second 0.2 m away is dropped, while the
  tracker's exact check had taken it as a zero-dt update. `make compare-tracks` labels
  such vessels.
- DARK_GAP on real traffic now excludes gaps in port and far from boundaries and
  protected areas. It still does not exclude gaps at the edge of receiver coverage,
  which the spec also asks for; those remain the main source of real-traffic alerts
  that are not switch-offs.
- `flink_leaks` gives a running check of the Flink job from the outside. It only sees
  what the tracker's checks can see: an exact repeat, not a near-repeat Flink missed.
- The tracker now depends on the Flink job for input. Running it without Flink needs
  `TRACKER_INPUT_TOPIC=ais.raw`, and then raises no DARK_GAP.
