# Results

Measured on an Apple M1 laptop (8 GB RAM, macOS 26.5), Docker VM with 8 CPUs and 3.9 GB, using the 2026-10-28 DMA day. Dates are when each number was taken. Rows that name `make reconcile` or `make flink-check` were measured with those commands, before the reconciliation job (ADR 0021) replaced both; the job runs the same checks as `raw_stored` and `flink_balance`.

## Detections

Tracker on `ais.enriched` with the zone condition, 700,000 rows at 10x (2026-11-04, run `20261103T221808Z-476749`):

| Run | DARK_GAP TP / FP / FN | TELEPORT TP / FP / FN | Alerts on real vessels |
|---|---|---|---|
| clean, 10 injected events (4 decoys) | 3 / 0 / 0 | 3 / 0 / 0 | 13 DARK_GAP, 5 TELEPORT |

Each DARK_GAP hit carries the zone it was raised on: DARK 1 at 4.0 nm from a protected area, DARK 2 at 6.1 nm from an EEZ boundary, DARK 3 at 3.4 nm from a protected area, the same distances PostGIS gives. The far-from-everything and in-port decoys raised nothing. 24 rows in `dvw.alerts FINAL`, 24 distinct `alert_id`.

Precision and recall are 1.00 for both types on this set. That says the rules fire once on the events they are meant for and not on the decoys. On real traffic the zone condition cut DARK_GAP from 17 (the same rows without it) to 13; the 13 have not been checked against receiver coverage.

## Latency

The 5 s p99 target holds in steady state but not over the whole 700k run. In the first spike Flink itself wrote records up to 6.3 s after they were sent; in the second Flink wrote on time and the delay came after, at the checkpoint commit or in the tracker. The cause of either is not known yet ([Known issues](architecture.md#known-issues)). Full-day throughput is still to be measured.

## All measurements

| What | Command | Result |
|---|---|---|
| Replay 10,000 rows at 100x | `make replay ARGS="--limit 10000 --speed 100"` | 3.1 s, 10,000 acknowledged |
| Replay 200,000 rows at 100x | `make replay ARGS="--limit 200000 --speed 100"` | 13.5 s and 13.8 s (two runs), 200,000 acknowledged each. Paced by the 100x schedule, not a throughput limit |
| Raw sink, re-reading a backlog | `rpk group seek dvw-raw-sink --to start`, then start the sink | 129,811 messages in 3.5 s, about 37,000 rows/s. 50,000-row inserts take 240 to 300 ms, offset commits 1 to 6 ms |
| Raw sink memory | `docker stats` after the backlog | 138 MiB of its 384 MiB limit |
| Zero loss, whole topic | `count() FINAL` vs topic high watermarks | 529,812 messages on `ais.raw`, 529,812 rows, every offset present once |
| Tracker, reading the existing backlog | start the tracker on 529,812 `ais.raw` messages | about 4,200 records/s (one worker). Counters add up to 529,812. 150 MiB of its 512 MiB limit |
| Tracker counters, 10,000-row replay at 10x (2026-11-03) | `make replay ARGS="--limit 10000"`, then `make tracker-logs` | `read=10000` = 4,075 updated + 1,279 restarted (vessels already tracked in an earlier run) + 3,507 duplicate + 910 not_a_ship + 229 no_position. 5,354 track messages. The 910 non-ship records are 682 coast stations and 228 aids to navigation, all still in `dvw.ais_raw`. Replay took 7.5 s. `make reconcile` OK on all 6 partitions |
| Tracker, 10,000-row replay (2026-11-02, before the ship filter) | `make replay ARGS="--limit 10000 --speed 100"` | 5,684 track messages (the rest are repeated fixes and missing positions), one per `(src_partition, src_offset)`. Innovation median 0.9 m, p99 22 m |
| Zero loss with the sink killed mid-run | `docker kill` at 66,093 of 200,000 stored, then restart | `make reconcile` OK: 200,000 produced = 200,000 stored |
| Replay 700,000 rows + 1,665 synthetic at 50x | `make replay ARGS="--limit 700000 --speed 50 --inject ..."` | 74 to 77 s, 701,665 acknowledged. About 4 s of that is the up-front span check |
| Injected anomalies, tracker killed mid-run (tracker on `ais.raw`, 50x) | `docker kill dvw-tracker` after the 3rd injected alert, 370,000 records unprocessed, then `make tracker` | DARK_GAP 3 / 0 / 0 and TELEPORT 3 / 0 / 0, no extras, same 28 alerts as the clean runs. 3,823 records skipped as `redelivered` on restart. `make reconcile` OK |
| Alerts topic vs table | 3 runs, then 6 alerts resent byte-identical to simulate a tracker resend | 90 messages on `ais.alerts`, 90 rows without `FINAL`, 84 with `FINAL` = 84 distinct `alert_id` |
| Zone package build, from the pinned sources (2026-11-03) | `make refdata-build` | 14 s. Peak memory of the largest cut (WDPA) 691 MiB including page cache, limit 1 GB. A second build from scratch gave byte-identical files |
| Zone package load into PostGIS under Rosetta (2026-11-03) | `make refdata-load`, three runs | 6.8 to 10.9 s each: verify 0.1 s, staging load and checks 6.3 to 9.7 s, swap 36 to 61 ms. 2,353 zones and 103 boundary lines |
| Zone package load with the EPSG:25832 copies (2026-11-03) | `make refdata`, twice | staging load and checks 13.1 to 13.8 s, swap 40 to 59 ms. `zones` and `zones_25832` 2,353 rows each, the boundary tables 103 each. `zones_25832` has 3,026,879 vertices (3,019,186 in `zones`) and takes 49 MB with indexes |
| Zone package load with the 256-vertex pieces (2026-11-04) | `make refdata-load` | staging load and checks 38.7 s, swap 47 ms. 34,535 zone pieces (3,109,058 vertices) and 171 boundary pieces. Reading all pieces as WKB takes 0.15 s in PostGIS, against 18 s for cutting at read time |
| Nearest EEZ boundary: 25832 alone, nearest point in 25832 then geodesic, geography (2026-11-03) | `ST_Distance` in 25832; `ST_ClosestPoint` in 25832, then `::geography` to it; `::geography` in 4326 | Kattegat (57.0 N, 11.6 E) 9,045.9 / 9,046.6 / 9,046.6 m. Bornholm area (55.3 N, 15.5 E) 19,355.2 / 19,324.5 / 19,324.5 m. 17.3 E 3,169.6 / 3,160.2 / 3,160.2 m |
| Whole zone pipeline offline (2026-11-03) | `UV_OFFLINE=1 make refdata`, package deleted first | 19.8 s for build, verify and load |
| `vessels_current`, one 10,000-row replay into an empty table (2026-11-03) | `TRUNCATE vessels_current`, group seeked to the end, `make replay ARGS="--limit 10000"` | 5,354 track messages, 1,279 rows = 1,279 distinct MMSIs in the run's tracks. Upserts of 1,000 to 3,300 tracks take 38 to 139 ms |
| `vessels_current`, same run reprocessed (2026-11-03) | `rpk group seek dvw-vessels-sink --to-file` the run's start offsets | 5,354 messages, `written=0 stale=1279`. `md5` of the whole table, `updated_at` included, identical before and after |
| `vessels_current`, a fix 1 h older than the stored one (2026-11-03) | one hand-made message on `ais.tracks` for MMSI 211772470 | `written=0 stale=1`, row unchanged |
| Vessels sink, re-reading the `ais.tracks` backlog (2026-11-03) | first start, about 1.95 million track messages from earlier runs | about 55,000 messages/s, 25 to 33 ms per 10,000-message upsert. 72 MiB of its 192 MiB limit |
| Flink, first start on the `ais.raw` backlog (2026-11-04) | `make flink-up` with no committed offsets | 3.1 million messages from earlier runs in about 2 minutes, roughly 25,000 to 30,000 messages/s on one TaskManager. 30 checkpoints, none failed. p50 0.3 s, p99 3.8 s while catching up, about 0.2 s once caught up. State 12 MB |
| Flink check, 10,000 rows at 10x (2026-11-04) | `make replay ARGS="--limit 10000"`, `make flink-check RUN=...` | OK on all 6 partitions: 10,000 = 5,683 enriched + 294 DLQ (all `position_unavailable`) + 4,023 duplicates. No offset twice. A replay of the same rows from before Flink existed, read from the backlog, gives identical numbers |
| Flink check, TaskManager killed mid-run (2026-11-04) | 10,000 rows at 1x, `docker kill dvw-flink-taskmanager` 20 s in, `docker start` 25 s later | Job restored checkpoint 75. `make flink-check` OK, same 5,683 / 294 / 4,023 as the clean run. Reading `ais.enriched` for that run: 5,786 records and 103 offsets twice with `read_uncommitted`, 5,683 and none twice with `read_committed` |
| Flink check, 200,000 rows at 10x (2026-11-04) | `make replay ARGS="--limit 200000"` | 104.9 s. `make flink-check` OK: 117,277 enriched + 1,756 DLQ + 80,967 duplicates. `make reconcile` OK on the raw store too |
| Zone tagging, started on a backlog (2026-11-04) | `make flink-savepoint`, `make replay SPEED=100 ARGS="--limit 300000"`, `make flink-restart` | Zone source read 34,706 pieces in 12.6 s (2.7 s on an idle stack). Shared index built in 0.6 to 0.9 s. 58,344 records buffered and flushed across 4 tasks. `make flink-check` OK: 176,245 enriched + 2,303 DLQ + 121,452 duplicates, 0 untagged |
| Zone tags against PostGIS (2026-11-04) | `make zone-check RUN=20261103T200718Z-7f33b0` | 400 records (122 in a protected area, 176 in port): 0 differing `eez_id`, `mpa_ids` or `in_port`. Largest difference 0.10 m for `dist_eez_boundary_m`, 0.52 m for `dist_mpa_m` |
| Zone index cost (2026-11-04) | The job's jar in a separate JVM: heap after GC before and after building, then 200,000 random points in the area | 65 MB retained, 115 MB at the build's peak. 36 µs per lookup: containment, two nearest searches, three projections, two geodesic distances |
| Flink check after savepoint, restart and replay (2026-11-04) | `make flink-savepoint`, `make flink-restart`, `make replay SPEED=100 ARGS="--limit 100000"` | OK on all 6 partitions: 58,241 enriched + 1,178 DLQ + 40,581 duplicates, no offset twice, 0 untagged |
| Memory with everything running (2026-11-04) | `docker stats` every 5 s during the 200,000-row replay, Console on too | Total 1.9 to 2.1 GiB of the 3.9 GiB VM. TaskManager 660 MiB (935 MiB peak after the 3.1 million backlog), ClickHouse 614, JobManager 352, Redpanda 276, tracker 92, the rest under 75 each |
| Tracker on `ais.enriched`, same 10,000 rows as from `ais.raw` (2026-11-04) | replay with the tracker on `ais.raw`, switch, replay again, `make compare-tracks A=... B=...` | 1,278 of 1,279 vessels identical, 1 differing by Flink's 5-decimal dedup. Tracker `read=5683` = Flink's enriched count, `flink_leaks=0` |
| End-to-end latency at 10x, 10,000 rows (2026-11-04) | `make replay ARGS="--limit 10000"`, `make latency RUN=...` | send to track sent: p50 2.2 s, p99 3.5 s, max 3.6 s (5,353 tracks) |
| End-to-end latency at 10x, 701,815 rows (2026-11-04) | `make replay ARGS="--limit 700000 --inject ..."`, `make latency RUN=...` | send to track sent: p50 2.8 s, p99 8.2 s, max 10.4 s (392,977 tracks). Per 30 s window p99 is 3.4 to 4.9 s, except two windows at 9.0 and 8.6 s. Send to TELEPORT stored in ClickHouse: max 3.0 s (8 alerts) |
| Tracks and rollups, 10,000 rows at 10x (2026-11-04) | `make replay ARGS="--limit 10000"` | 5,353 tracks. `count()` with `FINAL` in `dvw.tracks` = `sum(n_fixes)` in `ais_1min_v` = in `ais_1hour_v` = 5,353 |
| Rollups after re-inserting every track (2026-11-04) | `rpk group seek dvw-tracks-sink --to start`, restart the sink | 2,833,322 tracks re-inserted in about 96 s (about 29,500 rows/s with both views). For all 27 runs, the counts and a hash of every 1-minute and 1-hour row are identical before and after. One insert hit ClickHouse's memory limit and succeeded on the first retry |
| Tracks and rollups, 900,000 rows at 100x (2026-11-04) | `make replay SPEED=100 ARGS="--limit 900000"` | 52 s to replay, 502,264 tracks up to 01:14:59 event time. `count()` with `FINAL` = `sum(n_fixes)` at both resolutions = 502,264 |
| Rollup backfill (2026-11-04) | `make backfill-rollups` | 26 runs, 2.8 million tracks. Largest chunk 37 MiB. Run twice on the same run: numbers unchanged |
| Memory during the 900,000-row replay (2026-11-04) | `docker stats` every 5 s | Peak 2.65 GiB in total of the 3.9 GiB VM. ClickHouse 988 MiB of 1.25 GiB, TaskManager 925, Redpanda 337, JobManager 278, raw sink 198, tracker 151, tracks and alerts sink 123 of 320, the rest under 90 each |
| Reconciliation job, 10,000 rows at 10x (2026-11-04) | `make replay ARGS="--limit 10000"`, `make reconcile-run RUN=...` | OK on all 6 partitions in 33 s, most of it waiting for the tracker: 10,000 stored, 5,683 enriched + 294 DLQ + 4,023 duplicates, 5,353 tracks = 5,353 rollup fixes. 31 rows in `dvw.reconciliation`; a rerun still reads 31 with `FINAL` |
| Reconciliation started during a replay (2026-11-04) | `make replay SPEED=2 ARGS="--limit 10000"`, `make reconcile-run` 22 s in, `BASELINE=` the run above | Waited for the replay to complete, then about 50 s for the tracker. OK, same numbers, and 1,279 of 1,279 vessels' tracks match the baseline. Send to track stored p99 4,972 ms |
| Reconciliation, TaskManager and raw sink killed mid-run (2026-11-04) | 10,000 rows at 1x, `docker kill dvw-flink-taskmanager dvw-raw-sink` 15 s in, `docker start` both 20 s later | Job restored checkpoint 891. OK on all 6 partitions, same counts as the clean run. The latency row warns (p99 52 s, the outage) |
| Reconciliation with three broken lines (2026-11-04) | a 10,000-row CSV with a bad MMSI, a `garbage` latitude and a 0,0 position, `make replay AIS_CSV=...` | All three in the DLQ as `bad_mmsi`, `no_position`, `null_island`; every check OK. One row then deleted from `dvw.ais_raw`: `raw_stored` FAIL, missing 1, exit 1 |
