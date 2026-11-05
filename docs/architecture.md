# Architecture

How the pipeline is put together: the components, the topics between them, and what each one guarantees about delivery. The diagram is in the [README](../README.md#architecture). Measured numbers are in [results.md](results.md), and the reasoning behind each choice is in the [ADRs](decisions.md).

## Components

| Component | Code | Reads | Writes |
|---|---|---|---|
| Replayer | `ingest/replayer/` | the pinned DMA AIS day (CSV) | `ais.raw`, `data/runs/<run_id>/produced.json` |
| Raw sink | `sinks/clickhouse/` | `ais.raw` | `dvw.ais_raw` |
| Flink enrichment job | `flink/enrichment/` (Java 17, Flink 1.20) | `ais.raw`, the active zone package in PostGIS | `ais.enriched`, `ais.dlq`, `ais.dedup_counts` |
| Tracker | `stream/faust_app/` (Faust) | `ais.enriched` | `ais.tracks`, `ais.alerts` |
| Tracks and alerts sink | `sinks/clickhouse/` | `ais.tracks`, `ais.alerts` | `dvw.tracks` (and its rollups), `dvw.alerts` |
| Vessels sink | `sinks/postgis_vessels/` | `ais.tracks` | `vessels_current` in PostGIS |
| Zone package loader | `refdata/` | the signed zone package | zone tables in PostGIS ([data.md](data.md)) |
| Reconciliation job | `reconcile/job.py` | every topic above, ClickHouse | `dvw.reconciliation` ([operations.md](operations.md#reconciliation)) |

Everything runs in Docker Compose on one 8 GB laptop, offline once images and data are present. Each service has a memory limit; the budget is at the top of `deploy/compose/docker-compose.yml`.

## Topics

Every topic has exactly 6 partitions. Vessel topics (`ais.raw`, `ais.enriched`, `ais.tracks`, `ais.alerts`) are keyed by MMSI; `ais.dedup_counts` is keyed by `run_id`, and `ais.dlq` keeps the key of the record it rejects, which can be `invalid` or null. Every producer uses the Java-compatible murmur2 partitioner, so a vessel's records are in the same partition number on every vessel topic ([ADR 0001](adr/0001-kafka-partitioner-murmur2.md)). `init-topics` fails unless each topic has 6 partitions.

| Topic | Producer | Content |
|---|---|---|
| `ais.raw` | replayer | one message per CSV row, with `run_id`, `trace_id` and `replay_send_ts` ([ADR 0002](adr/0002-replayer-ordering-and-raw-contract.md)) |
| `ais.enriched` | Flink | valid, deduplicated records with `src_partition`/`src_offset`, `station_class` and zone tags |
| `ais.dlq` | Flink | rejected records with a reason code, the original payload and the raw partition and offset |
| `ais.dedup_counts` | Flink | duplicates dropped, per run, raw partition and 10 s window ([ADR 0016](adr/0016-dedup-counts-topic.md)) |
| `ais.tracks` | tracker | one message per accepted fix: filtered position and velocity, innovation |
| `ais.alerts` | tracker | DARK_GAP and TELEPORT alerts, each with a deterministic `alert_id` |

## Delivery guarantees

| Stage | Guarantee | How |
|---|---|---|
| Replayer → `ais.raw` | every acknowledged message counted | idempotent producer; `produced.json` records the acknowledged offset range per partition |
| Raw sink → `dvw.ais_raw` | at-least-once into an idempotent table | offsets committed only after the insert succeeds; `ReplacingMergeTree` keyed on partition and offset ([ADR 0003](adr/0003-raw-sink-python-consumer.md)) |
| Flink → `ais.enriched`, `ais.dlq`, `ais.dedup_counts` | exactly-once | 3 s checkpoints; transactional sinks commit with the checkpoint ([ADR 0014](adr/0014-flink-exactly-once-3s-checkpoints-5s-latency.md)) |
| Tracker → `ais.tracks`, `ais.alerts` | at-least-once, never lost | reads `read_committed`; outputs acknowledged before state is written; a raw-offset bookmark skips redelivered records ([ADR 0004](adr/0004-tracker-at-least-once-outputs-before-state.md), [ADR 0019](adr/0019-tracker-on-enriched-and-dark-gap-zone-condition.md)) |
| Tracks and alerts sink | idempotent | `dvw.tracks` deduplicated on the fix's identity, `dvw.alerts` on `alert_id` ([ADR 0006](adr/0006-alerts-sink-config-driven.md), [ADR 0020](adr/0020-rollups-from-tracks-and-ttl.md)) |
| Vessels sink | idempotent | upsert that only moves a row forward in event time ([ADR 0012](adr/0012-vessels-current-upsert-forward-in-event-time.md)) |

The reconciliation job checks these per partition for every run ([operations.md](operations.md#reconciliation)).

## Flink enrichment

The Flink job reads `ais.raw` and the active zone package, and writes three topics:

```
ais.raw -> parse + validate --invalid--------------------------------------------> ais.dlq
                 \--valid--> keyBy MMSI -> dedup --unique--> keyBy MMSI
                                            |                -> zone tagging ----> ais.enriched
PostGIS active package -> zone source -> broadcast --------------^
                                            \--duplicates--> keyBy (run, partition)
                                                -> 10 s event-time count ---------> ais.dedup_counts
```

**Validation.** Rules are checked in a fixed order, and the first failure decides the code: `decode_error`, `bad_mmsi`, `no_position`, `position_unavailable` (lat 91 / lon 181), `position_out_of_range`, `null_island`, `bad_timestamp` (more than a day off the replayed day, `AIS_DAY`), `sog_invalid` (negative, or above 102.2 kn other than the 102.3 "not available" code). Each DLQ record carries its code, a detail string, the original payload, and the raw partition and offset. Base stations, buoys and other non-ship stations are valid. They go to `ais.enriched` with a `station_class` from the same MMSI table the tracker uses. A position far outside the receiver area is valid too, because catching impossible jumps is TELEPORT's job.

**Dedup.** Two records are copies when they share run, MMSI, event time, and position rounded to 5 decimal places (about 1 m). A second ship using the same MMSI in the same second has a different position and survives, which is the evidence MMSI_CLASH will need. A key is remembered for 2 minutes of event time, and an event-time timer clears it ([ADR 0015](adr/0015-dedup-key-and-counted-duplicates.md)). About 40% of DMA rows are copies.

**Counting what is dropped.** Once dedup drops records, "dropped" and "lost" look the same unless the drops are counted exactly. Duplicates leave the dedup step through a side output. They are counted per run and raw partition in 10 s event-time windows and written to `ais.dedup_counts` through the same transactional sink. Windows also flush on a 5 s wall-clock timer, so the last ones of a run do not wait for the next run's data. The check is then:

```
raw(run, partition) = enriched + dlq + sum(dedup_counts.count)     over the run's ais.raw offset range
```

**Zone tagging.** Every record in `ais.enriched` gets `eez_id`, `mpa_ids`, `in_port`, `dist_eez_boundary_m`, `dist_mpa_m` (0 inside a protected area) and `zones_version` ([ADR 0018](adr/0018-flink-zone-tagging-read-at-startup.md)):

- *Startup gate.* The job is submitted only once PostGIS has an active package. `make flink-up` first loads the last built package if none is active, which is the case after `make nuke`; with no package built at all it says so and does not submit. I checked this by clearing the active marker: the submit sat at the gate, and `make refdata-load` released it within 5 s.
- *Read at startup.* A one-reader source reads the package's 34,706 pieces ([ADR 0017](adr/0017-zone-pieces-for-in-memory-lookup.md)) in one locked snapshot, in 2.7 s, and broadcasts them followed by a "zones complete" marker. It reads again on every restart, so a new package goes live with `make flink-restart` ([operations.md](operations.md#applying-a-new-zone-package)). Each record's `zones_version` says which package tagged it.
- *One index per TaskManager.* The first task to see the marker builds three JTS STRtrees (65 MB), and the other three tasks share that index. The index is deliberately not Flink state, because broadcast state is copied by every task at every checkpoint.
- *The buffer.* The AIS backlog reaches the tagging step long before the zones do. Until the marker arrives, records wait in per-vessel keyed state, and then they leave in order. Started on a 300,000-record backlog, the job held 58,344 records this way, and not one enriched record came out untagged.

Distances follow [ADR 0013](adr/0013-metric-zone-copy-and-active-package.md). The job finds the nearest point in UTM (EPSG:25832), then measures the geodesic distance to it. `make zone-check RUN=<run_id>` recomputes a sample of tags in PostGIS on the 4326 tables, with geography distances. A protected area's edges are straight in degrees, which is how the package and `ST_Contains` read them, but `::geography` reads each edge as a geodesic. Along long offshore edges that gave a 28 m difference until the check densified the polygons the same way.

**Exactly-once.** Checkpoints every 3 s snapshot the dedup state together with the Kafka offsets. The three Kafka sinks write in transactions that commit only when a checkpoint completes. After a crash, the job restarts from the last checkpoint, and the broker aborts whatever was written after it. When I killed the TaskManager mid-run, a `read_uncommitted` reader saw 103 records twice and a `read_committed` reader saw each once ([results.md](results.md)). The cost is latency: output waits up to 3 s for its checkpoint, so the end-to-end target is p99 under 5 s at 10x ([ADR 0014](adr/0014-flink-exactly-once-3s-checkpoints-5s-latency.md)).

**Deployment.** A session cluster in Compose: a JobManager (768 MB), one TaskManager with 4 slots (1280 MB), parallelism 4, heap state, and checkpoints on a Docker volume. A one-shot `flink-submit` container submits the job. Records keep the MMSI key and use the producer's default murmur2 partitioner, so each vessel stays in the same partition number it had in `ais.raw` ([ADR 0001](adr/0001-kafka-partitioner-murmur2.md)).

## Tracker

A Faust worker tracks every ship with a Kalman filter and writes one `ais.tracks` message per accepted fix: filtered position and velocity, and how far the fix landed from the prediction (the innovation). Base stations, buoys, SAR aircraft and distress beacons also send AIS. The tracker recognises them by MMSI range (ITU-R M.585) and skips them with a `not_a_ship` count; their rows stay in the raw store ([ADR 0008](adr/0008-10x-replay-target-and-ship-only-tracking.md)).

It reads `ais.enriched` (`TRACKER_INPUT_TOPIC`) with `read_committed`, through a small Faust transport, so it only sees output of completed Flink checkpoints. Faust only reads committed when its own output is transactional too, and the tracker stays at-least-once ([ADR 0019](adr/0019-tracker-on-enriched-and-dark-gap-zone-condition.md)). The transport sets a private aiokafka attribute, so at startup the tracker checks that its consumer really reads committed, and exits with code 78 if not. A test against Redpanda aborts one transaction and commits another; the tracker's consumer reads only the committed record.

- **The bookmark is a raw offset.** Each vessel's state remembers the last `ais.raw` offset applied, so a redelivered record is skipped ([ADR 0004](adr/0004-tracker-at-least-once-outputs-before-state.md)). On `ais.enriched` that offset comes from the record's `src_offset`, never its own offset: Flink drops about 40% of records, so enriched offsets run far behind raw ones and would look old. Tracks keep `src_partition`/`src_offset`, so every sink's dedup key is the raw identity.
- **Outputs before state.** A record's tracks and alerts are acknowledged by the broker before the vessel state is written, so a crash can repeat a track message but not lose one.
- **Safety net.** The tracker keeps its own validity, ship and duplicate checks. Anything they catch on `ais.enriched` is counted as a Flink leak (`leak_invalid`, `leak_station_class`, `leak_duplicate`, logged as `flink_leaks`). Over the 701,815-record run it stayed 0, but that run had no 98-prefix MMSI in it (see the next point), so a zero there does not prove the station classes agree.
- **One contract for both sides.** `contracts/ais_enriched.json` lists every station class (wire value, ship or not, example MMSIs) and the duplicate key's rounding. The Java and Python tests both load it, so changing a wire value fails both. It exists because the two drifted once: the tracker treated only `ship` as a ship, so a 98-prefix craft (a tender or RIB, tagged `craft_with_parent`) got no track. The whole AIS day has one such vessel, from 07:36.
- **Same tracks either way.** The same 10,000 rows read from `ais.raw` and then from `ais.enriched` gave identical tracks for 1,278 of 1,279 vessels (`make compare-tracks`). The other one sent two reports in the same second 0.2 m apart: Flink's dedup key rounds to 5 decimals and dropped the second, and the tracker compared positions exactly and took it. The tracker's duplicate check rounds the same way as Flink's key (ties up, like Java's `Math.round`); that comparison has not been rerun since.

The tracker's own accounting (every record read gets one skip reason or result) is in its logged counters, not stored per run.

## Detections

| Alert | Fires when | Carries |
|---|---|---|
| DARK_GAP | A vessel last moving faster than 2 kn has not been heard for more than 30 min of event time, and its last accepted fix was within 10 nm of an EEZ boundary or a protected area and not in port | Last filtered position, the Kalman-predicted position 30 min after the last fix, its uncertainty, and the zone tags of the last fix |
| TELEPORT | A fix implies more than 50 kn from the last accepted fix (ignoring jumps under 1 km or 10 s), or lands too far from the Kalman prediction (NIS above 50 and at least 1 km off) | The fix as received, the predicted position, which rule fired |

Both are raised by the tracker ([ADR 0005](adr/0005-detections-in-the-tracker-event-time.md)). The thresholds are settings in `stream/faust_app/config.py`.

- **Event time, not wall clock.** Silence is measured against a watermark: the highest `event_ts` seen per partition and replay run. When a partition's watermark moves into a new minute, the tracker scans that partition's vessels for gaps. A vessel that reports again after more than 30 min also raises the gap, in case no scan saw it. Both paths produce the same bytes, so a gap is one alert.
- **One alert per event.** DARK_GAP is raised once per silence. TELEPORT is raised on the first rejected fix of a jump. If the vessel keeps reporting from the new position, the third rejection restarts the filter there, without another alert.
- **The outlier does not reach the filter.** A rejected fix still counts as "heard", so a vessel sending implausible positions is not dark.
- **Deterministic ids.** `alert_id` is a UUIDv5 of type, MMSI, run and the fix that triggered it. A crash can make the tracker resend an alert. The resent copy has the same id, and `dvw.alerts` keeps one row.
- **Zones come from Flink.** The zone condition reads the tags the Flink job put on the vessel's last accepted fix (`dist_eez_boundary_m`, `dist_mpa_m`, `in_port`); the tracker never queries PostGIS. 10 nm is `DARK_GAP_ZONE_NM`. A rejected TELEPORT fix does not count, because its position is the one judged implausible. A record without tags (the tracker pointed at `ais.raw`) raises no DARK_GAP.

### Injected anomalies

`--inject ingest/replayer/anomalies.yaml` adds ten synthetic vessels (MMSIs 200000001 to 200000023, a block the real data does not use) ([ADR 0007](adr/0007-injected-anomalies-and-scoring.md)):

- three that switch AIS off for 45 min while under way, then reappear on their dead-reckoned track. They go dark 4.0 nm from a protected area, 6.1 nm from an EEZ boundary, and 3.4 nm from a protected area;
- three whose reported position jumps 30 km in one report and stays there;
- four decoys that must not alert: a moored vessel silent for 45 min, a 0.5 km jump, a vessel under way that goes dark in open North Sea water (38 nm from a boundary, 29 nm from a protected area), and one under way at 3 kn that goes dark inside Copenhagen port, next to a protected area.

Labels go to `data/runs/<run_id>/labels.json`. `make score RUN=<run_id>` compares them with `dvw.alerts FINAL` and prints TP, FP and FN per alert type. A match must be the exact `alert_id`, so an alert raised one report late counts as a miss. The events need 54 min of event time, which is about 650,000 rows of the DMA day (200,000 rows are only 16.7 min). The replayer refuses a run that is too short and says so. Scores are in [results.md](results.md#detections).

## Storage

### Raw store

The raw sink writes every `ais.raw` message to `dvw.ais_raw`, including ones that are not valid JSON (stored with a reason code instead of being dropped).

### Current positions

`vessels_current` in PostGIS holds one row per MMSI: the Kalman-filtered position (`geometry(Point, 4326)`, GiST-indexed), speed and course of the latest accepted fix, its `event_ts`, the `run_id` of the track message it came from, and `updated_at`, the wall time of the write. It is what the map will query.

The vessels sink reads `ais.tracks`, reduces each batch to the latest fix per MMSI, and upserts that in one statement and one transaction. It commits the Kafka offsets only after the transaction has committed. The upsert replaces a row only when the new `event_ts` is strictly later:

```sql
ON CONFLICT (mmsi) DO UPDATE SET ... WHERE excluded.event_ts > vessels_current.event_ts
```

That one condition is why replays cannot corrupt the table. A redelivered batch, a reset consumer group or a second replay of the same day carries fixes that are not later than what is stored, so they write nothing. That includes `updated_at`. An older run read after a newer one cannot move a vessel back. On a tie the first fix received wins, inside a batch and across batches, so the result does not depend on batch boundaries. The catch is that `run_id` names the run whose fix is stored, not the most recent run.

### History and retention

A vessel's day can be read at three resolutions. All three come from `ais.tracks`, so they hold ships only, with Kalman-filtered positions. Every record as received stays in `dvw.ais_raw`.

| Table | One row per | Kept |
|---|---|---|
| `dvw.tracks` | accepted fix | 7 days after it was stored |
| `dvw.ais_1min_v` | vessel, run and minute | 30 days after the minute was last written |
| `dvw.ais_1hour_v` | vessel, run and hour | kept |

The tracks and alerts sink stores `ais.tracks` in `dvw.tracks`, deduplicated on each fix's identity: MMSI, event time, and the raw partition and offset it came from. Two materialised views on `dvw.tracks` keep the 1-minute and 1-hour rollups up to date on every insert. Each rollup row holds the last fix's position, speed and course, min and max speed, the largest innovation, and `n_fixes`.

Materialised views run on every inserted block, before ClickHouse merges away duplicate rows, so a batch the sink inserts twice after a crash reaches the views twice. The rollups therefore use only aggregates that a second copy cannot change: the last fix by `argMax` over (event time, offset), min and max, and `n_fixes` as `uniqExact` of the fix's identity, never `count()` or `avg()` ([ADR 0020](adr/0020-rollups-from-tracks-and-ttl.md)). That makes the check exact: for any run, `sum(n_fixes)` at either resolution equals `count()` with `FINAL` in `dvw.tracks`.

One vessel at all three resolutions (put in your own `run_id` from `data/runs/`):

```sql
-- every fix
SELECT event_ts, lat, lon, sog_kn, cog_deg
FROM dvw.tracks FINAL
WHERE run_id = '20261103T232419Z-9959ca' AND mmsi = 273274080
ORDER BY event_ts, src_offset;

-- one row per minute; dvw.ais_1hour_v has the same columns, one row per hour
SELECT bucket, last_event_ts, lat, lon, sog_kn, cog_deg, min_sog_kn, max_sog_kn, n_fixes
FROM dvw.ais_1min_v
WHERE run_id = '20261103T232419Z-9959ca' AND mmsi = 273274080
ORDER BY bucket;
```

For that vessel, across the 01:00 boundary of the replayed day:

```
res                        last fix                  lat       lon       sog   n_fixes
tracks                     2026-10-28 00:59:58.000   57.7704   9.83173   6.3   1
1min  2026-10-28 00:59:00  2026-10-28 00:59:58.000   57.7704   9.83173   6.3   30
1hour 2026-10-28 00:00:00  2026-10-28 00:59:58.000   57.7704   9.83173   6.3   1743
```

The last fix of the 00:59 minute is the last fix of the 00:00 hour. Over the run's 75 minutes and 2 hours for this vessel, every bucket's last fix and `n_fixes` match what `dvw.tracks` gives, and each hour's `n_fixes` is the sum of its minutes.

Filter the rollup views by `run_id` and `mmsi`. Each view finishes its aggregates at read time, and reading every vessel of every run at once exceeds ClickHouse's memory limit on this laptop.

Retention is measured from `ingested_at`, when a row was stored, not from `event_ts`. The replayed day is in the past, so an event-time TTL would delete rows as they arrive. TTL deletes during merges, so expired rows can linger for a few hours. `make ttl-demo` shows it working on a scratch table with the same expression in minutes.

## Going faster

The pipeline is sized for replay at 10x on one worker per service ([ADR 0008](adr/0008-10x-replay-target-and-ship-only-tracking.md)). 10x is the target, not the ceiling of the design. One tracker worker reads about 4,200 records/s, and the DMA day averages about 219 rows/s of event time, so 10x (about 2,200 rows/s) leaves headroom and 50x (about 11,000 rows/s) does not. In order of cost, these are the ways to go faster:

1. **More tracker workers, up to 6.** `ais.enriched` has 6 partitions keyed by MMSI, and vessel state is partitioned the same way, so Faust can spread the partitions over up to 6 workers without code changes. Each worker costs about 150 to 512 MiB, which is what rules it out on this laptop.
2. **Drop numpy from the Kalman step.** The filter is 4x4, and per-record numpy calls cost more in overhead than the arithmetic does. Unrolling the predict and update steps into plain floats removes that overhead. The gain is not measured yet. It needs a test that compares the two versions so the outputs stay identical.
3. **Route non-ship stations elsewhere in Flink.** Non-ship stations are about 6% of enriched records. The tracker reads them only to skip them.

Past 6 workers, the next limit is the partition count. Changing it needs an ADR ([ADR 0001](adr/0001-kafka-partitioner-murmur2.md)).

## Known issues

- **No JobManager high availability.** A JobManager restart drops the running Flink job, and nothing restarts it: the tracker gets no input until `make flink-up` ([ADR 0022](adr/0022-flink-restores-newest-retained-state.md)). After a crash, `make flink-up` restores the job from its newest checkpoint, and nothing is lost or repeated. After a planned stop (`make down`, `docker stop` or `docker restart` of the JobManager) Flink has deleted the job's checkpoints, so it starts fresh from the committed offsets: no output should be lost (reasoned from the commit order, not yet measured), but the dedup keys are, and copies straddling the stop reach the tracker as `leak_duplicate`. Run `make flink-savepoint` before a planned stop to keep the state.
- **p99 latency over 5 s on long runs.** The 5 s target holds in steady state but not over the whole 700k run ([results.md](results.md#latency)). On 2026-11-04 even 10,000-row runs at 10x came out at p99 5.7 s to track sent (6.1 to 6.9 s to stored), against 3.5 s measured earlier the same day, so it varies with the state of the stack. On 2026-11-05 the results report ([2026-11-05-319c8bd](results/2026-11-05-319c8bd.md)) narrowed it to the tracker. On `ais.enriched` it read 500 to 1,000 records/s in the first minute of a 700k run at 10x, before any fault, against about 1,150/s of input, and logged event-loop timer drift throughout. Flink completed a checkpoint every 2 to 4 s over the same minute. Latency to track stored rose from 4 s to 31 s within that minute (p99 171 s over records sent before the first fault). That is about a quarter of the 4,200/s measured on `ais.raw` earlier. Profiling the tracker's per-record path and its DARK_GAP scans is the next step. The reconciliation job stores these percentiles for every run.
- **Tracker offset commits lagged once after a run.** After the 700k run the tracker had processed every record, but its committed offsets on two `ais.enriched` partitions stayed about 1,000 behind for several minutes, stuck at a gap left by Flink's transaction markers, then caught up. A 10,000-row run afterwards committed to the end within 30 s on every partition, so it is not systematic. Nothing is lost (a restart re-reads those records and the bookmark skips them), but `make score` can report INCOMPLETE and `make reconcile-run` keeps waiting meanwhile (up to `TIMEOUT`, 600 s by default). Suspected: Faust's offset-gap handling under `read_committed`.
- **ClickHouse has little memory headroom.** Its container limit is 1280 MiB, and ClickHouse caps itself at 90% of that (`max_server_memory_usage`, 1.13 GiB). It idles at 450 to 800 MiB and peaked at 988 MiB in the 900k run. An insert into `dvw.tracks` occasionally fails with `MEMORY_LIMIT_EXCEEDED` while a rollup merge runs; the sink's retry has always succeeded so far, and fail-stop would restart it otherwise. Reading a rollup view without a `run_id`/`mmsi` filter can exceed the limit. Trimming ClickHouse's system log tables and caches, or a larger limit, is the next step.
- **The tracker's skip reasons are not reconciled.** They are logged, not stored per run, so the reconciliation job checks what the tracker wrote but not what it skipped.
- **Ships using a non-ship MMSI are not tracked.** The tracker classifies stations by MMSI range (ITU-R M.585), so a vessel transmitting under a buoy, aid-to-navigation or base-station MMSI is skipped as `not_a_ship`. Its rows stay in `dvw.ais_raw`.
- **No monitoring stack runs locally, by design.** There are no metrics or dashboards in Compose; the components log their counters, and the reconciliation job stores each run's verdict and latency in `dvw.reconciliation`.
- **Not built yet:** DARK_GAP does not exclude gaps at the edge of receiver coverage, so real-traffic DARK_GAP counts still include coverage holes near boundaries and protected areas. MMSI_CLASH is not built.
