# Dark Vessel Watch

Ships report their position, speed and identity over AIS. Ships involved in illegal fishing, sanctions evasion or smuggling often hide by switching AIS off, faking their position, or sharing an identity (MMSI) with another ship. This project watches a day of real AIS traffic from Danish waters and flags those three behaviours as they happen.

It is a streaming pipeline that runs on a laptop without a network connection: Redpanda, Flink, Faust, ClickHouse and PostGIS, all in Docker Compose. It is built to keep per-vessel state, raise alerts within seconds, store the full history, and prove with a reconciliation job that no message was lost along the way.

## Where it stands

A day of Danish AIS traffic replays into Redpanda in event-time order, and every message lands in ClickHouse, including ones that are not valid JSON (they are stored with a reason code instead of being dropped). For each replay run, a per-partition check compares what the broker acknowledged with what is stored and shows that nothing was lost or duplicated. That holds when the sink is killed partway through a run or made to re-read the whole topic.

A Faust worker tracks every vessel with a Kalman filter and writes one `ais.tracks` message per accepted fix: filtered position and velocity, and how far the fix landed from the prediction (the innovation). It is at-least-once with outputs stored before state, so a crash can repeat a track message but not lose one ([ADR 0004](docs/adr/0004-tracker-at-least-once-outputs-before-state.md)).

The same worker raises DARK_GAP and TELEPORT alerts on `ais.alerts`, each with a deterministic `alert_id` so a reprocessed alert is the same alert ([ADR 0005](docs/adr/0005-detections-in-the-tracker-event-time.md)). The DARK_GAP zone check is a stub that always passes until the PostGIS zones arrive. The alerts table, Flink enrichment and the reference data are not built yet.

## Quickstart

Needs Docker with Compose v2, about 8 GB of free RAM, GNU make, and Python 3.12 with [uv](https://docs.astral.sh/uv/). One-time setup and data licensing notes are in [docs/SETUP.md](docs/SETUP.md).

```sh
cp .env.example .env
make up        # git hooks, stack, topics, ClickHouse migrations, raw sink, tracker
make ps        # every service healthy, raw-sink running
make fetch     # download and verify the pinned AIS day (once, about 3 GB)

make replay ARGS="--limit 10000 --speed 100"   # prints the run_id
make reconcile RUN=<run_id>                     # per-partition produced vs stored
```

Redpanda Console is at http://localhost:8080. `make sink-logs` follows the sink (one line per batch). `make tracker-logs` follows the tracker, which logs its counters every 30 s (`tracks=... duplicate=... no_position=...`). `make down` stops the stack and keeps the data. `make nuke` stops it and deletes all data volumes, including ClickHouse.

Other targets: `make migrate` applies new files in `sql/clickhouse/` and does nothing when up to date. `make test` and `make typecheck` run pytest and `mypy --strict`.

If the sink crashes, Docker restarts it and it re-reads from its last committed offset. `docker stop` and `docker kill` count as deliberate stops and leave it down; `make sink` starts it again.

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

End-to-end latency, alert precision and recall, and full-day throughput are still to be measured.
