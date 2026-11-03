# Dark Vessel Watch

Ships report their position, speed and identity over AIS. Ships involved in illegal fishing, sanctions evasion or smuggling often hide by switching AIS off, faking their position, or sharing an identity (MMSI) with another ship. This project watches a day of real AIS traffic from Danish waters and flags those three behaviours as they happen.

It is a streaming pipeline that runs on a laptop without a network connection: Redpanda, Flink, Faust, ClickHouse and PostGIS, all in Docker Compose. It is built to keep per-vessel state, raise alerts within seconds, store the full history, and prove with a reconciliation job that no message was lost along the way.

## Where it stands

A day of Danish AIS traffic replays into Redpanda in event-time order, and every message lands in ClickHouse, including ones that are not valid JSON (they are stored with a reason code instead of being dropped). For each replay run, a per-partition check compares what the broker acknowledged with what is stored and shows that nothing was lost or duplicated. That holds when the sink is killed partway through a run or made to re-read the whole topic.

A Faust worker tracks every ship with a Kalman filter and writes one `ais.tracks` message per accepted fix: filtered position and velocity, and how far the fix landed from the prediction (the innovation). It is at-least-once with outputs stored before state, so a crash can repeat a track message but not lose one ([ADR 0004](docs/adr/0004-tracker-at-least-once-outputs-before-state.md)). Base stations, buoys, SAR aircraft and distress beacons also send AIS. The tracker recognises them by MMSI range (ITU-R M.585) and skips them with a `not_a_ship` count. Their rows stay in the raw store ([ADR 0008](docs/adr/0008-10x-replay-target-and-ship-only-tracking.md)).

The pipeline is sized for replay at 10x on one worker per service, on an 8 GB laptop. That is the speed the latency target applies to and the speed `make replay` uses by default. [Going faster](#going-faster) explains what it would take to go further.

The same worker raises DARK_GAP and TELEPORT alerts on `ais.alerts`, each with a deterministic `alert_id` so a reprocessed alert is the same alert ([ADR 0005](docs/adr/0005-detections-in-the-tracker-event-time.md)). A second instance of the ClickHouse sink stores them in `dvw.alerts`, deduplicated on `alert_id` ([ADR 0006](docs/adr/0006-alerts-sink-config-driven.md)). The replayer can mix labelled synthetic vessels into a run, and a scorer checks that each injected anomaly raised exactly its alert ([ADR 0007](docs/adr/0007-injected-anomalies-and-scoring.md)). That holds when the tracker is killed partway through a run.

The zones DARK_GAP needs come from three reference datasets: Marine Regions EEZ polygons and boundary lines, protected areas from WDPA, and ports from the World Port Index. Each is pinned by SHA-256 in `refdata/sources.yaml`. All three are downloaded by hand: EEZ sits behind a form, WDPA moves monthly, and NGA refuses scripted WPI downloads. `make fetch-refdata` verifies them and prints exactly where to get and place any that are missing ([ADR 0009](docs/adr/0009-zone-sources-pinned-and-area-from-the-feed.md)). The zone package covers only the area the feed sees. `make area` computes that from the AIS day: 2.69 to 17.53 E, 53.52 to 59.57 N, which is the trimmed extent of 18.9 million valid positions plus a 20 nm margin. Plain min/max would have reached 60 S, because of 179 stray fixes.

`make refdata` turns those files into a signed zone package and loads it into PostGIS, offline ([Zones](#zones)). A package that fails its signature, a file hash or a staging check is refused as a whole, and the live zones stay as they were ([ADR 0010](docs/adr/0010-zone-package-staging-swap-and-port-radius.md), [ADR 0011](docs/adr/0011-signed-zone-package-built-from-pinned-sources.md)).

A third sink keeps `vessels_current` in PostGIS: one row per vessel with its latest filtered position, upserted from `ais.tracks`. A row only moves forward in event time, so replays, redeliveries and a reset consumer group cannot move a vessel back or rewrite its row ([Current positions](#current-positions), [ADR 0012](docs/adr/0012-vessels-current-upsert-forward-in-event-time.md)).

A Flink job now sits on `ais.raw` as well ([Flink enrichment](#flink-enrichment)). It validates every message, sending rejects to `ais.dlq` with one of eight reason codes. It drops repeats of the same report heard by several base stations, and writes the rest to `ais.enriched` with the raw partition and offset each record came from. It is exactly-once end to end: output becomes visible only when a 3 s checkpoint completes ([ADR 0014](docs/adr/0014-flink-exactly-once-3s-checkpoints-5s-latency.md)). Dropped duplicates are counted exactly-once into their own topic ([ADR 0015](docs/adr/0015-dedup-key-and-counted-duplicates.md), [ADR 0016](docs/adr/0016-dedup-counts-topic.md)), so a check can prove, per run and partition, that every raw message is enriched, rejected or a counted duplicate. That holds when the TaskManager is killed partway through a run.

The same job tags every record with its zones: EEZ, protected areas, port, and geodesic distances to the nearest EEZ boundary and protected area. It reads the active zone package from PostGIS when it starts, and records that arrive before the zones are held back rather than tagged "no zone" ([ADR 0018](docs/adr/0018-flink-zone-tagging-read-at-startup.md)). On a 400-record sample the tags agree with PostGIS exactly, and the distances agree to within 0.5 m.

Not built yet: moving the tracker from `ais.raw` to `ais.enriched`, the DARK_GAP zone check that reads these tags (it still passes everywhere), and MMSI_CLASH.

## Detections

| Alert | Fires when | Carries |
|---|---|---|
| DARK_GAP | A vessel last moving faster than 2 kn has not been heard for more than 30 min of event time | Last filtered position, the Kalman-predicted position 30 min after the last fix, and its uncertainty |
| TELEPORT | A fix implies more than 50 kn from the last accepted fix (ignoring jumps under 1 km or 10 s), or lands too far from the Kalman prediction (NIS above 50 and at least 1 km off) | The fix as received, the predicted position, which rule fired |

How it works, briefly:

- **Event time, not wall clock.** Silence is measured against a watermark: the highest `event_ts` seen per partition and replay run. When a partition's watermark moves into a new minute, the tracker scans that partition's vessels for gaps. A vessel that reports again after more than 30 min also raises the gap, in case no scan saw it. Both paths produce the same bytes, so a gap is one alert.
- **One alert per event.** DARK_GAP is raised once per silence. TELEPORT is raised on the first rejected fix of a jump. If the vessel keeps reporting from the new position, the third rejection restarts the filter there, without another alert.
- **The outlier does not reach the filter.** A rejected fix still counts as "heard", so a vessel sending implausible positions is not dark.
- **Deterministic ids.** `alert_id` is a UUIDv5 of type, MMSI, run and the fix that triggered it. A crash can make the tracker resend an alert. The resent copy has the same id, and `dvw.alerts` keeps one row.

Stubbed until the zones are loaded: the spec's DARK_GAP also requires the vessel to be near an EEZ boundary or protected area, not in port, and not at the edge of receiver coverage. Today that predicate returns true everywhere, so every qualifying gap fires, including coverage holes and gaps in port at more than 2 kn. Real-traffic DARK_GAP counts are therefore an upper bound. MMSI_CLASH is not implemented.

### Injected anomalies

`--inject ingest/replayer/anomalies.yaml` adds eight synthetic vessels (MMSIs 200000001 to 200000022, a block the real data does not use):

- three that switch AIS off for 45 min while under way, then reappear on their dead-reckoned track;
- three whose reported position jumps 30 km in one report and stays there;
- two decoys that must not alert: a moored vessel silent for 45 min, and a 0.5 km jump.

Labels go to `data/runs/<run_id>/labels.json`. `make score RUN=<run_id>` compares them with `dvw.alerts FINAL` and prints TP, FP and FN per alert type. A match must be the exact `alert_id`, so an alert raised one report late counts as a miss.

The events need 54 min of event time, which is about 650,000 rows of the DMA day (200,000 rows are only 16.7 min). The replayer refuses a run that is too short and says so.

Results on the 2026-10-28 day, 700,000 rows at 50x, three runs:

| Run | DARK_GAP TP / FP / FN | TELEPORT TP / FP / FN | Alerts on real vessels |
|---|---|---|---|
| clean | 3 / 0 / 0 | 3 / 0 / 0 | 17 DARK_GAP, 5 TELEPORT |
| clean, again | 3 / 0 / 0 | 3 / 0 / 0 | 17 DARK_GAP, 5 TELEPORT |
| tracker killed (`SIGKILL`) after the 3rd injected alert, 370,000 records unprocessed, then restarted | 3 / 0 / 0 | 3 / 0 / 0 | 17 DARK_GAP, 5 TELEPORT |

Precision and recall are 1.00 for both types on this set. That says the rules fire once on the events they are meant for and not on the two decoys. It says nothing yet about real traffic, where the 17 gaps have not been checked against coverage or ports.

## Zones

DARK_GAP needs to know whether a vessel went quiet near an EEZ boundary or a protected area, and not in port. Those shapes come from three datasets, cut to the feed's area at full detail:

| Layer | From | Rows | Vertices |
|---|---|---|---|
| EEZ polygons (`zones`, kind `eez`) | Marine Regions World EEZ v12 | 8 | 85,526 |
| EEZ boundary lines (`eez_boundaries`) | Marine Regions World EEZ v12 | 103 | 13,339 |
| Protected areas (`zones`, kind `mpa`) | WDPA November 2026, realm Coastal or Marine | 2,169 | 2,922,333 |
| Ports (`zones`, kind `port`) | World Port Index 2019, each point a 2 nm circle | 176 | 11,327 |

Vertices are `SUM(ST_NPoints(geom))` per layer after loading. Protected areas account for 96% of them, because Natura 2000 sites follow the coastline closely. That is no problem for PostGIS. It is the first number to look at before Flink keeps zones in memory.

How a package is made and loaded:

1. **Build** (`make refdata-build`). The build re-checks the SHA-256 of every source in `refdata/sources.yaml`. `ogr2ogr` then cuts each layer to `refdata/area.yaml`, in a container with no network. Port circles are computed on the WGS84 ellipsoid, and each vertex is within 0.5 m of 2 nm as PostGIS geography measures it. The output is one GeoPackage per layer and a `manifest.json` with every input and file hash, signed with minisign. The package version is a hash of the inputs, and two builds from the same inputs are byte-identical.
2. **Verify** (`make refdata-verify`). The signature over the manifest is checked against the committed `refdata/keys/dvw.pub`, then every file's hash. Changing one byte in any file fails this step.
3. **Load** (`make refdata-load`). The loader verifies again, then fills a staging schema, checks row counts and geometry validity there, writes the metric copies described below, and swaps all the tables into place. All of that is one transaction. Each load adds a row to `refdata_packages` and marks it as the one active package. Loading the same package twice gives identical zones and two registry rows.

The Flink enrichment job keeps the zones in memory and measures with JTS, which is planar. So the loader also writes `zones_25832` and `eez_boundaries_25832`: the same rows in EPSG:25832 (UTM zone 32N, the Danish metric grid), already repaired, checked and GiST-indexed ([ADR 0013](docs/adr/0013-metric-zone-copy-and-active-package.md)). Before reprojecting, it adds vertices to long segments: geodesic for boundary lines, so a metric distance matches a geography distance, and straight in degrees for polygons, so containment matches `zones`. Without that, a 50 km boundary segment becomes a different line in UTM and a Kattegat distance was 6 m off. With it, what remains is the projection's scale error: 0.7 m on 9 km in the Kattegat, but 0.16% around Bornholm and about 0.3% at the eastern edge of the area. So the job uses 25832 only to find the nearest point, then reports the geodesic distance to it. Near Bornholm that gives 19,324.5 m, the same as PostGIS geography, where UTM alone says 19,355.2 m.

One protected area alone has 977,787 vertices, and a point-in-polygon test scans all of them. So the loader also cuts every metric zone and boundary into pieces of at most 256 vertices (`ST_Subdivide`) and checks that each zone's pieces add up to its area and each boundary's to its length. That gives 34,535 zone pieces and 171 boundary pieces. A lookup in the job is then a tree query plus a scan of a few hundred vertices ([ADR 0017](docs/adr/0017-zone-pieces-for-in-memory-lookup.md)).

```sql
SELECT package_version FROM refdata_packages WHERE active;   -- the live package
```

To find the nearest boundary in metres, order by geography distance. `ORDER BY geom <-> point` on geometry ranks in degrees, and at 57 N it puts a 37.6 km line ahead of a 30.2 km one:

```sql
SELECT id, round(ST_Distance(geom::geography, p::geography)) AS metres
FROM eez_boundaries, ST_SetSRID(ST_Point(11.3, 56.9), 4326) AS p   -- mid-Kattegat
ORDER BY 2 LIMIT 1;                                                -- line 1120, 30,241 m
```

## Current positions

`vessels_current` holds one row per MMSI: the Kalman-filtered position (`geometry(Point, 4326)`, GiST-indexed), speed and course of the latest accepted fix, its `event_ts`, the `run_id` of the track message it came from, and `updated_at`, the wall time of the write. It is what the map and the DARK_GAP zone check will query.

The vessels sink reads `ais.tracks`, reduces each batch to the latest fix per MMSI, and upserts that in one statement and one transaction. It commits the Kafka offsets only after the transaction has committed. The upsert replaces a row only when the new `event_ts` is strictly later:

```sql
ON CONFLICT (mmsi) DO UPDATE SET ... WHERE excluded.event_ts > vessels_current.event_ts
```

That one condition is why replays cannot corrupt the table. A redelivered batch, a reset consumer group or a second replay of the same day carries fixes that are not later than what is stored, so they write nothing. That includes `updated_at`. An older run read after a newer one cannot move a vessel back. On a tie the first fix received wins, inside a batch and across batches, so the result does not depend on batch boundaries. The catch is that `run_id` names the run whose fix is stored, not the most recent run. To start over, truncate the table and reset the `dvw-vessels-sink` group.

## Flink enrichment

The Flink job (Java 17, Flink 1.20 DataStream API, `flink/enrichment/`) reads `ais.raw` and the active zone package, and writes three topics:

```
ais.raw -> parse + validate --invalid--------------------------------------------> ais.dlq
                 \--valid--> keyBy MMSI -> dedup --unique--> keyBy MMSI
                                            |                -> zone tagging ----> ais.enriched
PostGIS active package -> zone source -> broadcast --------------^
                                            \--duplicates--> keyBy (run, partition)
                                                -> 10 s event-time count ---------> ais.dedup_counts
```

**Validation.** Rules are checked in a fixed order, and the first failure decides the code: `decode_error`, `bad_mmsi`, `no_position`, `position_unavailable` (lat 91 / lon 181), `position_out_of_range`, `null_island`, `bad_timestamp` (more than a day off the replayed day, `AIS_DAY`), `sog_invalid`. Each DLQ record carries its code, a detail string, the original payload, and the raw partition and offset. Base stations, buoys and other non-ship stations are valid. They go to `ais.enriched` with a `station_class` from the same MMSI table the tracker uses. A position far outside the receiver area is valid too, because catching impossible jumps is TELEPORT's job.

**Dedup.** Two records are copies when they share run, MMSI, event time, and position rounded to 5 decimal places (about 1 m). A second ship using the same MMSI in the same second has a different position and survives, which is the evidence MMSI_CLASH will need. A key is remembered for 2 minutes of event time, and an event-time timer clears it ([ADR 0015](docs/adr/0015-dedup-key-and-counted-duplicates.md)). About 40% of DMA rows are copies.

**Counting what is dropped.** Once dedup drops records, "dropped" and "lost" look the same unless the drops are counted exactly. Duplicates leave the dedup step through a side output. They are counted per run and raw partition in 10 s event-time windows and written to `ais.dedup_counts` through the same transactional sink. Windows also flush on a 5 s wall-clock timer, so the last ones of a run do not wait for the next run's data. The check is then:

```
raw(run, partition) = enriched + dlq + sum(dedup_counts.count)     over the run's ais.raw offset range
```

`make flink-check RUN=<run_id>` reads all three topics with `read_committed`. It also fails if any raw offset appears twice across `ais.enriched` and `ais.dlq`, or if any enriched record has no zone tags.

**Zone tagging.** Every record in `ais.enriched` gets `eez_id`, `mpa_ids`, `in_port`, `dist_eez_boundary_m`, `dist_mpa_m` (0 inside a protected area) and `zones_version`. How the zones get there ([ADR 0018](docs/adr/0018-flink-zone-tagging-read-at-startup.md)):

- *Startup gate.* `make flink-up` waits until PostGIS has an active package, and says so. I checked this by clearing the active marker: the submit sat at the gate, and `make refdata-load` released it within 5 s.
- *Read at startup.* A one-reader source reads the package's 34,706 pieces (ADR 0017) in one locked snapshot, in 2.7 s, and broadcasts them followed by a "zones complete" marker. It reads again on every restart, so a new package goes live by stopping with a savepoint, loading, and `make flink-restart`. Each record's `zones_version` says which package tagged it.
- *One index per TaskManager.* The first task to see the marker builds three JTS STRtrees (65 MB), and the other three tasks share that index. The index is deliberately not Flink state, because broadcast state is copied by every task at every checkpoint.
- *The buffer.* The AIS backlog reaches the tagging step long before the zones do. Until the marker arrives, records wait in per-vessel keyed state, and then they leave in order. Started on a 300,000-record backlog, the job held 58,344 records this way, and not one enriched record came out untagged.

Distances follow ADR 0013. The job finds the nearest point in UTM, then measures the geodesic distance to it. `make zone-check RUN=<run_id>` recomputes a sample of tags in PostGIS on the 4326 tables, with geography distances. One subtlety came up there. A protected area's edges are straight in degrees, which is how the package and `ST_Contains` read them, but `::geography` reads each edge as a geodesic. Along long offshore edges that gave a 28 m difference until the check densified the polygons the same way.

**Exactly-once.** Checkpoints every 3 s snapshot the dedup state together with the Kafka offsets. The three Kafka sinks write in transactions that commit only when a checkpoint completes. After a crash, the job restarts from the last checkpoint, and the broker aborts whatever was written after it. When I killed the TaskManager mid-run, a `read_uncommitted` reader saw 103 records twice and a `read_committed` reader saw each once ([Numbers](#numbers)). The cost is latency: output waits up to 3 s for its checkpoint, so the end-to-end target is now p99 under 5 s at 10x, not 2 s ([ADR 0014](docs/adr/0014-flink-exactly-once-3s-checkpoints-5s-latency.md)).

**Running it.** It is a session cluster in Compose: a JobManager (768 MB), one TaskManager with 4 slots (1280 MB), parallelism 4, heap state, and checkpoints on a Docker volume. A one-shot `flink-submit` container submits the job. To fit that in the 3.9 GiB Docker VM, Redpanda dropped to 512 MB and ClickHouse to 1.25 GB, and Console only runs on demand. Records keep the MMSI key and use the producer's default murmur2 partitioner, so each vessel stays in the same partition number it had in `ais.raw` (ADR 0001).

## Quickstart

Needs Docker with Compose v2, about 8 GB of free RAM, GNU make, and Python 3.12 with [uv](https://docs.astral.sh/uv/). The AIS data is not in the repository: `make fetch` downloads it and checks its SHA-256.

```sh
cp .env.example .env
make up        # git hooks, stack, topics, migrations, the three sinks, tracker
make ps        # every service healthy, raw-sink running
make fetch     # download and verify the pinned AIS day (once, about 3 GB)

make replay ARGS="--limit 10000"                # 10x by default; prints the run_id
make reconcile RUN=<run_id>                     # per-partition produced vs stored
```

The Flink job (about 2 GB more; the image build compiles the job and runs its tests):

```sh
make flink-up                                   # JobManager, TaskManager, submit the job; UI on http://localhost:8081
make replay ARGS="--limit 10000"
make flink-check RUN=<run_id>                   # per partition: raw = enriched + dlq + dups, no offset twice, all tagged
make zone-check RUN=<run_id>                    # a sample of zone tags recomputed in PostGIS

docker kill dvw-flink-taskmanager               # mid-replay; then bring it back:
docker start dvw-flink-taskmanager              # the job restores the last checkpoint; flink-check still OK

make flink-savepoint                            # stop the job with a savepoint (sinks commit everything)
make flink-restart                              # rebuild, stop with a savepoint if running, start from the newest one
make flink-logs
```

The first start reads `ais.raw` from the beginning, because there are no committed offsets yet. Later starts resume from the checkpoint or savepoint. Change `AIS_DAY` only together with `make flink-restart`: the `bad_timestamp` rule is fixed when the job is submitted.

Labelled anomalies:

```sh
# needs at least ~650k rows, see Detections.
# 50x only to shorten the run; the tracker falls behind and catches up, and the score waits for it.
make replay ARGS="--limit 700000 --speed 50 --inject ingest/replayer/anomalies.yaml"
make score RUN=<run_id> WAIT=600                # TP/FP/FN per alert type
```

Reference data for the zones (the tracker does not read them yet):

```sh
make fetch-refdata   # verify the EEZ, WDPA and WPI files; prints where to download any that are missing
make refdata-image   # once, online: the GDAL + minisign image the build runs in
make refdata-key     # once: signing key in ~/.minisign/dvw.key (refuses if refdata/keys/dvw.pub exists)
make refdata         # offline: build, verify and load the zone package into PostGIS
make area            # only if the AIS day changes: recompute refdata/area.yaml from the CSV
```

`refdata/keys/dvw.pub` in the repository belongs to my key, so on another machine delete it before `make refdata-key` and use your own. `make refdata-load PKG=data/refdata/packages/<version>` loads an older package again, which is how a rollback works.

EEZ (Marine Regions form), WDPA (Protected Planet terms page) and WPI (NASA Harvest mirror of the NGA release) have to be downloaded by hand into `data/refdata/incoming/<source>/<version>/`, under the file names in `refdata/sources.yaml`. `make fetch-refdata` prints the page and the exact path for each missing file, and fails on any file whose SHA-256 does not match `refdata/sources.yaml`.

`make console` starts Redpanda Console on http://localhost:8080. It is not part of `make up`, to save 256 MB. `make sink-logs`, `make alerts-sink-logs` and `make vessels-sink-logs` follow the three sinks (one line per batch; the vessels sink also logs how many rows each batch `written` and how many were `stale`). `make tracker-logs` follows the tracker, which logs its counters every 30 s (`tracks=... read=... not_a_ship=... duplicate=...`). Every record read gets exactly one skip reason or result, so those counters add up to `read`. `make down` stops the stack and keeps the data. `make nuke` stops it and deletes all data volumes, including ClickHouse.

Other targets: `make migrate` and `make migrate-postgis` apply new files in `sql/clickhouse/` and `sql/postgis/` and do nothing when up to date (`make up` runs both). `make test` runs pytest and the Flink job's JUnit tests (in Docker, so no JDK or Maven needed on the host). `make typecheck` runs `mypy --strict`.

If a sink or the tracker crashes, Docker restarts it and it re-reads from its last committed offset. `docker stop` and `docker kill` count as deliberate stops and leave it down; `make sink`, `make alerts-sink`, `make vessels-sink` or `make tracker` starts it again. For the TaskManager, use `docker start dvw-flink-taskmanager`.

## Going faster

10x is the target, not the ceiling of the design. One tracker worker reads about 4,200 records/s, and the DMA day averages about 219 rows/s of event time, so 10x (about 2,200 rows/s) leaves headroom and 50x (about 11,000 rows/s) does not. In order of cost, these are the ways to go faster:

1. **More tracker workers, up to 6.** `ais.raw` has 6 partitions keyed by MMSI, and vessel state is partitioned the same way, so Faust can spread the partitions over up to 6 workers without code changes. Each worker costs about 150 to 512 MiB, which is what rules it out on this laptop.
2. **Drop numpy from the Kalman step.** The filter is 4x4, and per-record numpy calls cost more in overhead than the arithmetic does. Unrolling the predict and update steps into plain floats removes that overhead. I have not measured the gain yet. It needs a test that compares the two versions so the outputs stay identical.
3. **Filter earlier, in Flink.** Today the tracker decodes every record, including the 9% from non-ship stations and the 35 to 40% repeated reports. The Flink job already drops the repeats and rejects before `ais.enriched`, and marks non-ship stations. Once the tracker reads `ais.enriched`, it will only see records it uses.

Past 6 workers, the next limit is the partition count. Changing it needs an ADR (ADR 0001).

## Architecture

_To be written._

## Design decisions

_To be written. Each decision has an ADR in [docs/adr/](docs/adr/)._

## Numbers

Measured on 2026-11-02 on an Apple M1 laptop (8 GB RAM, macOS 26.5), Docker VM with 8 CPUs and 3.9 GB, using the 2026-10-28 DMA day.

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
| Injected anomalies, tracker killed mid-run | `docker kill dvw-tracker` after the 3rd injected alert, `make tracker` | 6 of 6 alerts, no extras, same 28 alerts as the clean runs. 3,823 records skipped as `redelivered` on restart. `make reconcile` OK |
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

End-to-end latency and full-day throughput are still to be measured.

## Attribution

The pipeline runs on data I do not own. None of it is in this repository. `make fetch` and `make fetch-refdata` get it from the source, and the hashes in `refdata/sources.yaml` pin the exact files.

- **AIS positions:** Contains AIS data from the Danish Maritime Authority. Free re-use under the Danish act on re-use of public sector information.
- **EEZ polygons and boundaries:** Flanders Marine Institute (2023). Maritime Boundaries Geodatabase: Maritime Boundaries and Exclusive Economic Zones (200NM), version 12. Available online at https://www.marineregions.org/. CC BY-NC-SA 4.0.
- **Marine protected areas:** UNEP-WCMC and IUCN (2026), Protected Planet: The World Database on Protected Areas (WDPA), November 2026, Cambridge, UK: UNEP-WCMC and IUCN. Available at: www.protectedplanet.net. Non-commercial use; no redistribution.
- **Ports:** National Geospatial-Intelligence Agency, World Port Index (Pub 150); obtained via NASA Harvest Portal. US Government work, public domain (original); CC BY on the mirror.

Because of the WDPA and Marine Regions terms, the built zone package is never committed or published. The repository ships the fetcher, the pinned hashes, the package builder and the public key instead. Anyone else builds and signs their own package from their own downloads.
