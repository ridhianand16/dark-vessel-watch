# Dark Vessel Watch

Ships report their position, speed and identity over AIS. Ships involved in illegal fishing, sanctions evasion or smuggling often hide by switching AIS off, faking their position, or sharing an identity (MMSI) with another ship. This project watches a day of real AIS traffic from Danish waters and flags those three behaviours as they happen.

It is a streaming pipeline that runs on a laptop without a network connection: Redpanda, Flink, Faust, ClickHouse and PostGIS, all in Docker Compose. It is built to keep per-vessel state, raise alerts within seconds, store the full history, and prove with a reconciliation job that no message was lost along the way.

## Where it stands

The local stack runs: Redpanda, Redpanda Console, ClickHouse and PostGIS start in Compose and wait until each is healthy. Topic setup is checked at startup. The init job creates the topics and fails if any of them has the wrong partition count. Nothing flows through the pipeline yet.

## Quickstart

Needs Docker with Compose v2, about 8 GB of free RAM, GNU make, and Python 3.12 with [uv](https://docs.astral.sh/uv/). One-time setup and data licensing notes are in [docs/SETUP.md](docs/SETUP.md).

```sh
cp .env.example .env
make up      # installs git hooks, starts the stack, creates topics
make ps      # every service should show as healthy
```

Redpanda Console is at http://localhost:8080. `make down` stops the stack and keeps the data. `make nuke` stops it and deletes all data volumes.

## Architecture

_To be written._

## Design decisions

_To be written. Each decision has an ADR in [docs/adr/](docs/adr/)._

## Numbers

_To be written: throughput, end-to-end latency, alert precision and recall, and reconciliation results._
