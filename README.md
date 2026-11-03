# Dark Vessel Watch

Ships report their position, speed and identity over AIS. Ships involved in illegal fishing, sanctions evasion or smuggling often hide by switching AIS off, faking their position, or sharing an identity (MMSI) with another ship. This project watches a day of real AIS traffic from Danish waters and flags those three behaviours as they happen.

It is a streaming pipeline that runs on a laptop without a network connection: Redpanda, Flink, Faust, ClickHouse and PostGIS, all in Docker Compose. It is built to keep per-vessel state, raise alerts within seconds, store the full history, and prove with a reconciliation job that no message was lost along the way.

## Where it stands

A day of Danish AIS traffic replays into Redpanda in event-time order, and every message lands in ClickHouse, including ones that are not valid JSON (they are stored with a reason code instead of being dropped). For each replay run, a per-partition check compares what the broker acknowledged with what is stored and shows that nothing was lost or duplicated. That holds when the sink is killed partway through a run or made to re-read the whole topic.

A Faust worker tracks every vessel with a Kalman filter and writes one `ais.tracks` message per accepted fix: filtered position and velocity, and how far the fix landed from the prediction (the innovation). It is at-least-once with outputs stored before state, so a crash can repeat a track message but not lose one ([ADR 0004](docs/adr/0004-tracker-at-least-once-outputs-before-state.md)).

The same worker raises DARK_GAP and TELEPORT alerts on `ais.alerts`, each with a deterministic `alert_id` so a reprocessed alert is the same alert ([ADR 0005](docs/adr/0005-detections-in-the-tracker-event-time.md)). A second instance of the ClickHouse sink stores them in `dvw.alerts`, deduplicated on `alert_id` ([ADR 0006](docs/adr/0006-alerts-sink-config-driven.md)). The replayer can mix labelled synthetic vessels into a run, and a scorer checks that each injected anomaly raised exactly its alert ([ADR 0007](docs/adr/0007-injected-anomalies-and-scoring.md)). That holds when the tracker is killed partway through a run.

Not built yet: PostGIS zones (so the DARK_GAP zone check passes everywhere), MMSI_CLASH, Flink enrichment and the reference data package.

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

Stubbed until the zones exist (M3): the spec's DARK_GAP also requires the vessel to be near an EEZ boundary or protected area, not in port, and not at the edge of receiver coverage. Today that predicate returns true everywhere, so every qualifying gap fires, including coverage holes and gaps in port at more than 2 kn. Real-traffic DARK_GAP counts are therefore an upper bound. MMSI_CLASH is not implemented.

### Injected anomalies

`--inject ingest/replayer/anomalies.yaml` adds eight synthetic vessels (MMSIs 999000001 to 999000022, a block the real data does not use):

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

## Quickstart

Needs Docker with Compose v2, about 8 GB of free RAM, GNU make, and Python 3.12 with [uv](https://docs.astral.sh/uv/). One-time setup and data licensing notes are in [docs/SETUP.md](docs/SETUP.md).

```sh
cp .env.example .env
make up        # git hooks, stack, topics, ClickHouse migrations, raw sink, tracker
make ps        # every service healthy, raw-sink running
make fetch     # download and verify the pinned AIS day (once, about 3 GB)

make replay ARGS="--limit 10000 --speed 100"   # prints the run_id
make reconcile RUN=<run_id>                     # per-partition produced vs stored

# with labelled anomalies (needs at least ~650k rows, see Detections)
make replay ARGS="--limit 700000 --speed 50 --inject ingest/replayer/anomalies.yaml"
make score RUN=<run_id> WAIT=600                # TP/FP/FN per alert type
```

Redpanda Console is at http://localhost:8080. `make sink-logs` and `make alerts-sink-logs` follow the two sinks (one line per batch). `make tracker-logs` follows the tracker, which logs its counters every 30 s (`tracks=... duplicate=... no_position=...`). `make down` stops the stack and keeps the data. `make nuke` stops it and deletes all data volumes, including ClickHouse.

Other targets: `make migrate` applies new files in `sql/clickhouse/` and does nothing when up to date. `make test` and `make typecheck` run pytest and `mypy --strict`.

If a sink or the tracker crashes, Docker restarts it and it re-reads from its last committed offset. `docker stop` and `docker kill` count as deliberate stops and leave it down; `make sink`, `make alerts-sink` or `make tracker` starts it again.

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
| Tracker, 10,000-row replay | `make replay ARGS="--limit 10000 --speed 100"` | 5,684 track messages (the rest are repeated fixes and missing positions), one per `(src_partition, src_offset)`. Innovation median 0.9 m, p99 22 m |
| Zero loss with the sink killed mid-run | `docker kill` at 66,093 of 200,000 stored, then restart | `make reconcile` OK: 200,000 produced = 200,000 stored |
| Replay 700,000 rows + 1,665 synthetic at 50x | `make replay ARGS="--limit 700000 --speed 50 --inject ..."` | 74 to 77 s, 701,665 acknowledged. About 4 s of that is the up-front span check |
| Injected anomalies, tracker killed mid-run | `docker kill dvw-tracker` after the 3rd injected alert, `make tracker` | 6 of 6 alerts, no extras, same 28 alerts as the clean runs. 3,823 records skipped as `redelivered` on restart. `make reconcile` OK |
| Alerts topic vs table | 3 runs, then 6 alerts resent byte-identical to simulate a tracker resend | 90 messages on `ais.alerts`, 90 rows without `FINAL`, 84 with `FINAL` = 84 distinct `alert_id` |

End-to-end latency and full-day throughput are still to be measured.
