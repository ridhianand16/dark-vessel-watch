# Operations

How to set up, run and check the pipeline. What each component does is in [architecture.md](architecture.md), and the data it needs is in [data.md](data.md).

## Requirements

| Tool | Version | Notes |
|---|---|---|
| Docker with Compose v2 (OrbStack on macOS) | latest | Everything runs in Compose. On an 8 GB host the 3.9 GiB Docker VM fits the stack; 5 to 6 GiB leaves headroom for package builds and full-day replays. |
| Python with [uv](https://docs.astral.sh/uv/) | 3.12 | The replayer, the reconcile tools and the zone package commands run on the host. |
| GNU make | any | Every entry point is a Makefile target. |
| JDK 17 and Maven 3.9 | optional | `make test` builds the Flink job and runs its tests in Docker. |
| rpk, GDAL, minisign | optional | Each runs in a container (`docker exec dvw-redpanda rpk ...`, the `refdata-tools` image). |

Python dependencies are in `pyproject.toml` and locked in `uv.lock`. Copy `.env.example` to `.env` before the first `make up`. The PostGIS container reads `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB`, and the application builds its DSN from those plus `POSTGRES_HOST` and `POSTGRES_PORT`.

Every image tag is pinned. Each service has a `mem_limit`; the budget and what each limit covers are in the comment at the top of `deploy/compose/docker-compose.yml`. Measured use is in [results.md](results.md).

## Make targets

**Stack**

| Target | What it does |
|---|---|
| `make up` | git hooks, Redpanda, ClickHouse, PostGIS, topics, migrations, the sinks, the tracker and the Flink job |
| `make ps` | container status |
| `make down` | stop the stack, keep the data volumes |
| `make nuke` | stop the stack and delete every data volume, ClickHouse included |
| `make console` | Redpanda Console on http://localhost:8080 (256 MB, so not part of `make up`) |
| `make migrate`, `make migrate-postgis` | apply new files in `sql/clickhouse/` and `sql/postgis/`; no-op when up to date (`make up` runs both) |
| `make topics` | (re)create the topics; idempotent, fails unless each has 6 partitions |

**Data and replay**

| Target | What it does |
|---|---|
| `make fetch` | download and verify the pinned AIS day (once, about 3 GB) |
| `make replay` | replay the AIS day into `ais.raw` at `SPEED` (default 10x); extra flags in `ARGS`, e.g. `ARGS="--limit 10000"`; prints the `run_id` |
| `make fetch-refdata` | verify the EEZ, WDPA and WPI files; prints where to download any that are missing |
| `make refdata` | build, verify and load the zone package, offline ([data.md](data.md#zone-package)) |
| `make refdata-image`, `make refdata-key` | once: the GDAL + minisign image (online), and the signing key |
| `make area` | recompute `refdata/area.yaml`; only when the AIS day or `refdata/package.yaml` changes |

**Checks**

| Target | What it does |
|---|---|
| `make reconcile-run RUN=<run_id>` | prove zero loss for one run, per partition ([below](#reconciliation)) |
| `make score RUN=<run_id> WAIT=600` | TP/FP/FN per alert type for a run replayed with `--inject` |
| `make latency RUN=<run_id>` | replayer send to track sent, to track stored, and to TELEPORT stored |
| `make zone-check RUN=<run_id>` | a sample of the run's zone tags recomputed in PostGIS |
| `make compare-tracks A=<run_id> B=<run_id>` | two runs of the same rows, vessel by vessel |
| `make backfill-rollups` | fill the rollups from tracks stored before the views existed; safe to repeat, also while the sink runs |
| `make ttl-demo` | the `ingested_at` TTL deleting rows, on a scratch table |
| `make test`, `make typecheck` | pytest and the Flink job's JUnit tests (in Docker), and `mypy --strict` |

**Logs**

| Target | Follows |
|---|---|
| `make sink-logs` | the raw sink: one line per batch |
| `make tracks-alerts-sink-logs` | the tracks and alerts sink: one line per batch |
| `make vessels-sink-logs` | the vessels sink: per batch, how many rows were `written` and how many were `stale` |
| `make tracker-logs` | the tracker's counters every 30 s; they add up to `read`, and `flink_leaks` should stay 0 |
| `make flink-logs` | the JobManager and TaskManager; the web UI is on http://localhost:8081 |

## Restarts

If a sink or the tracker crashes, Docker restarts it and it re-reads from its last committed offset. `docker stop` and `docker kill` count as deliberate stops and leave it down; `make sink`, `make tracks-alerts-sink`, `make vessels-sink` or `make tracker` starts it again. For the TaskManager, use `docker start dvw-flink-taskmanager`: the job restores its last checkpoint.

## Flink job

`make up` starts the Flink cluster through `make flink-up`, which submits the job unless it is already running. The job needs an active zone package. `make flink-up` loads the last one built if none is active, including after `make nuke`. On a fresh clone there is none yet: it says so and leaves the job unsubmitted, so build one (`make fetch-refdata refdata`), then run `make flink-up`.

```sh
make flink-savepoint     # stop the job with a savepoint; the sinks commit their last transactions
make flink-restart       # rebuild, stop with a savepoint if running, start from the newest state
```

`make flink-up` never replaces a running JobManager, because a session cluster keeps no jobs across a restart. Job code changes go live with `make flink-restart`. Every start, `make flink-up` included, restores the newest savepoint or retained checkpoint on the `flink-data` volume, and says which ([ADR 0022](adr/0022-flink-restores-newest-retained-state.md)). Only when there is none, on a fresh clone or after `make nuke`, does the job start fresh: from the committed offsets, or from the beginning of `ais.raw`. Change `AIS_DAY` only together with `make flink-restart`: the `bad_timestamp` rule is fixed when the job is submitted.

To run without Flink, set `TRACKER_INPUT_TOPIC=ais.raw` in `.env` and run `make tracker`: tracks work, DARK_GAP does not.

### Switching the tracker between topics

Stop the tracker, then `rpk group seek dvw-tracker --to end --topics ais.enriched --allow-new-topics`, then start it. Without the seek it starts at the earliest offset and re-tracks every earlier run.

## Reconciliation

Every replay run writes `data/runs/<run_id>/produced.json`: for each of the 6 `ais.raw` partitions, the first and last offset the broker acknowledged and how many messages that was. That file is the ground truth. Everything else is counted against it, per partition, over exactly that offset range, so another run's records cannot hide a gap in this one. A total would also hide one partition losing what another gained.

`make reconcile-run RUN=<run_id>` does it in three steps ([ADR 0021](adr/0021-reconciliation-job.md)):

1. **Wait for the run to finish.** The job first waits until the replayer marks the run `complete`, and refuses one that was interrupted or failed. Then it waits until each consumer group on the path has committed past the run's last record: the raw sink and Flink on `ais.raw`, the tracker on `ais.enriched`, and the tracks, vessels and alerts sinks after it. It waits for the run's last *record*, not the end of the topic. Flink's transaction markers sit at the end of `ais.enriched`, and the tracker cannot commit past them until new data arrives.
2. **Check that it adds up**, per partition:

   | Check | Proves |
   |---|---|
   | `raw_stored` | produced = rows in `dvw.ais_raw FINAL`, none from another run |
   | `flink_balance` | raw = enriched + DLQ + counted duplicates, and no raw offset written twice |
   | `zone_tagged` | no enriched record left without zone tags |
   | `rollup` | `sum(n_fixes)` in the 1-minute rollup = tracks in `dvw.tracks FINAL` |
   | `dlq_share` | warns when more than 5% went to the DLQ, with the count per reason |

   Plus one run-wide `latency` row: p50/p95/p99 from replayer send to the fix stored in `dvw.tracks` (warns above 5 s). With `BASELINE=<run_id>` of the same rows, `tracks_match` also compares every vessel's tracks with that run's.
3. **Keep the verdict.** One row per run, partition and check goes into `dvw.reconciliation`, with the counts and DLQ reasons. Running it again replaces the verdict instead of adding to it. The exit code is 1 if any check fails.

This is a real run: 10,000 rows with three broken lines added on purpose (an MMSI that is not a number, a latitude of `garbage`, a position at 0,0). All three land in the DLQ under their own reason, and everything still balances. Check rows that are the same on every partition are cut to one:

```
$ make reconcile-run RUN=20261103T234818Z-b4cfbc
run 20261103T234818Z-b4cfbc
partition  produced    stored  enriched    dlq    dups untagged   tracks   rollup
---------------------------------------------------------------------------------
        0      1665      1665       938     49     678        0      867      867
        1      1839      1839      1032     40     767        0      988      988
        2      1736      1736      1037     30     669        0      998      998
        3      1429      1429       861     32     536        0      822      822
        4      1800      1800       996     57     747        0      903      903
        5      1531      1531       818     89     624        0      774      774
---------------------------------------------------------------------------------
    total     10000     10000      5682    297    4021        0     5352     5352
dlq reasons: position_unavailable 294, bad_mmsi 1, no_position 1, null_island 1

check          partition  expected    actual  status note
---------------------------------------------------------
raw_stored             4      1800      1800  OK
flink_balance          4      1800      1800  OK
zone_tagged            4       996       996  OK
rollup                 4       903       903  OK
dlq_share              4      1800        57  OK     3.2% in dlq (position_unavailable 54, bad_mmsi 1, no_position 1, null_island 1)
dlq_share              5      1531        89  WARN   5.8% in dlq (position_unavailable 89), above 5%
latency              run      5352      5352  WARN   replay send to track stored: p50 3965 ms, p95 5782 ms, p99 6078 ms, max 6185 ms; p99 above 5000 ms
reconcile: wrote 31 rows to dvw.reconciliation
reconcile: OK, 10000 produced, every check balances on all 6 partitions (2 warnings)
```

The two warnings are real. Partition 5 carries more `lat 91` "not available" reports than the others. The latency is the open p99 issue in [Known issues](architecture.md#known-issues). To check that the job really catches a loss, I deleted one row of this run from `dvw.ais_raw`. It then failed with `raw_stored 2 ... missing 1` and exit code 1.

To test recovery, kill a component mid-replay and reconcile afterwards. At 10x, 10,000 rows replay in about 7.5 s, too fast to interrupt, so replay at 1x (about 75 s) and kill from a second terminal while it runs:

```sh
make replay SPEED=1 ARGS="--limit 10000"        # terminal 1; prints the run_id
docker kill dvw-flink-taskmanager               # terminal 2, while the replay runs
docker start dvw-flink-taskmanager              # the job restores the last checkpoint
make reconcile-run RUN=<run_id>                 # still OK on every partition
```

## Applying a new zone package

Zones reach DARK_GAP only through the Flink job's tags, and the job reads the active package once at start. So:

```sh
make refdata          # build, verify and load the new package; it becomes the active one
make flink-restart    # stop with a savepoint, start from it: the job re-reads the active package
```

Records tagged before the restart keep the old package's tags, and every record says which one in `zones_version`; so does each DARK_GAP's `details.zone`. The tracker needs no restart.

Rollback is the same with an older package: `make refdata-load PKG=data/refdata/packages/<version>`, then `make flink-restart`.

## Resetting a table

`vessels_current` keeps the latest fix per vessel across runs. To start over, truncate the table and reset the `dvw-vessels-sink` group. To verify a single run, start from an empty table: one filled by earlier runs holds vessels the new run never sees.
