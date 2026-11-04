# Dark Vessel Watch

Ships report their position, speed and identity over AIS. Ships involved in illegal fishing, sanctions evasion or smuggling often hide by switching AIS off, faking their position, or sharing an identity (MMSI) with another ship. This project replays a day of real AIS traffic from Danish waters and flags vessels that go dark near EEZ boundaries and protected areas (DARK_GAP) or jump to impossible positions (TELEPORT), as it happens.

It is a streaming pipeline that runs on a laptop without a network connection: Redpanda, Flink, Faust, ClickHouse and PostGIS, all in Docker Compose. It keeps per-vessel state, raises alerts within seconds, stores the full history, and proves with a reconciliation job, per partition and per run, that no message was lost or duplicated, including when components are killed mid-run.

## Running it

Needs Docker with Compose v2, about 8 GB of free RAM, GNU make, and Python 3.12 with [uv](https://docs.astral.sh/uv/). The AIS data is not in the repository: `make fetch` downloads it and checks its SHA-256.

```sh
cp .env.example .env
make up        # git hooks, stack, topics, migrations, the sinks, tracker, Flink job
make ps        # every service healthy
make fetch     # download and verify the pinned AIS day (once, about 3 GB)
```

The Flink job needs a zone package in PostGIS, built from three datasets you download by hand ([docs/data.md](docs/data.md#manual-downloads)). Until one exists, `make up` says so and leaves the job unsubmitted.

```sh
make fetch-refdata   # verify the EEZ, WDPA and WPI files; prints where to download any that are missing
make refdata-image   # once, online: the GDAL + minisign image the build runs in
make refdata-key     # once: signing key in ~/.minisign/dvw.key (delete refdata/keys/dvw.pub first)
make refdata         # offline: build, verify and load the zone package into PostGIS
make flink-up        # submit the Flink job once a package is active
```

Replay and check a run:

```sh
make replay ARGS="--limit 10000"                # 10x by default; prints the run_id
make reconcile-run RUN=<run_id>                 # waits for the pipeline, then proves zero loss per partition
make zone-check RUN=<run_id>                    # a sample of zone tags recomputed in PostGIS
```

Labelled anomalies (needs at least about 650,000 rows; about 6 minutes at 10x):

```sh
make replay ARGS="--limit 700000 --inject ingest/replayer/anomalies.yaml"
make score RUN=<run_id> WAIT=600                # TP/FP/FN per alert type, and the zone of each DARK_GAP
make latency RUN=<run_id>                       # replayer send to track sent, and to TELEPORT stored
```

Kill a component mid-replay and reconcile again. Replay at 1x so there is time to kill it, and run the kill from a second terminal:

```sh
make replay SPEED=1 ARGS="--limit 10000"        # terminal 1: about 75 s; prints the run_id
docker kill dvw-flink-taskmanager               # terminal 2, while the replay runs
docker start dvw-flink-taskmanager              # the job restores its last checkpoint
make reconcile-run RUN=<run_id>
```

`make test` runs pytest and the Flink job's JUnit tests (in Docker, so no JDK needed). `make typecheck` runs `mypy --strict`. `make down` stops the stack and keeps the data. Every target is described in [docs/operations.md](docs/operations.md).

## Documentation

- [Architecture](docs/architecture.md): components, topics, delivery guarantees, detections, storage, known issues
- [Data](docs/data.md): sources, licences and attribution, manual downloads, the zone package
- [Operations](docs/operations.md): make targets, reconciliation, Flink, applying a new zone package
- [Results](docs/results.md): every measurement, with date and command
- [Design decisions](docs/decisions.md): one line per ADR

## Attribution

Contains AIS data from the Danish Maritime Authority. Zones are built from Marine Regions World EEZ v12 (CC BY-NC-SA 4.0), the WDPA from UNEP-WCMC and IUCN (non-commercial, no redistribution) and the World Port Index from NGA via the NASA Harvest Portal. None of this data is in the repository; full attribution is in [docs/data.md](docs/data.md#licences-and-attribution).
