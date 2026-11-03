COMPOSE := docker compose -f deploy/compose/docker-compose.yml --env-file .env

.PHONY: hooks up down nuke topics ps console fetch fetch-refdata area refdata refdata-image refdata-key refdata-build refdata-verify refdata-load replay migrate migrate-postgis sink sink-logs tracks-alerts-sink tracks-alerts-sink-logs vessels-sink vessels-sink-logs tracker tracker-logs reconcile score flink-up flink-savepoint flink-restart flink-logs flink-check compare-tracks latency test test-py test-flink typecheck

.env:
	@echo "Missing .env. Run: cp .env.example .env" >&2; exit 1

SERVICES := redpanda clickhouse postgis
# Every profile, so down and nuke also remove Console and the one-shot tool containers.
ALL_PROFILES := --profile console --profile tools

# Read only the keys we need from .env rather than including the whole file.
env_get = $(shell sed -n 's/^$(1)=//p' .env 2>/dev/null)
AIS_DAY ?= $(call env_get,AIS_DAY)
AIS_DATA_DIR ?= $(or $(call env_get,AIS_DATA_DIR),./data/ais)
AIS_CSV ?= $(AIS_DATA_DIR)/aisdk-$(AIS_DAY).csv
REFDATA_DIR ?= $(or $(call env_get,REFDATA_DIR),./data/refdata)

## Point git at the repo's hooks (commit-msg format and trailer stripping).
hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/*

## Start the whole pipeline: Redpanda, ClickHouse, PostGIS, topics, migrations, the
## sinks, the tracker (reads ais.enriched) and the Flink enrichment job that
## writes ais.enriched. init-topics is run separately: `up --wait` treats a one-shot
## container that exits (even with 0) as a failure.
up: hooks .env
	$(COMPOSE) up -d --wait --wait-timeout 180 $(SERVICES)
	$(COMPOSE) run --rm init-topics
	$(MAKE) migrate migrate-postgis
	$(COMPOSE) up -d --build --remove-orphans raw-sink tracks-alerts-sink vessels-sink tracker
	$(MAKE) flink-up

## Stop and remove containers; named volumes (topic data, DBs) are kept.
down: .env
	$(COMPOSE) $(ALL_PROFILES) down

## Stop and delete everything including volumes. Destroys all data.
nuke: .env
	$(COMPOSE) $(ALL_PROFILES) down -v --remove-orphans

## (Re)create topics. Idempotent: safe to run any number of times.
topics: .env
	$(COMPOSE) run --rm init-topics

ps: .env
	$(COMPOSE) ps -a

## Redpanda Console on http://localhost:8080. Not started by `make up` (256 MB).
console: .env
	$(COMPOSE) --profile console up -d --wait console

## Download, verify (SHA-256) and unzip the pinned DMA AIS day into AIS_DATA_DIR.
fetch: .env
	@test -n "$(AIS_DAY)" || { echo "AIS_DAY is not set in .env" >&2; exit 1; }
	uv run python -m refdata.fetch --source dma_ais --day $(AIS_DAY) --dest $(AIS_DATA_DIR)

## Verify (SHA-256) the reference layers for the zone package in
## REFDATA_DIR/incoming/<source>/<version>/. EEZ and WDPA are placed by hand; WPI is
## downloaded if possible. A missing file prints where to get it and where to put it.
REFDATA_SOURCES := eez wdpa_marine wpi
fetch-refdata: .env
	uv run python -m refdata.fetch --incoming $(REFDATA_DIR)/incoming $(addprefix --source ,$(REFDATA_SOURCES))

## Recompute refdata/area.yaml (the zone package's bounding box) from the pinned AIS
## day. Only needed when the day or refdata/package.yaml changes; commit the result.
area: .env
	uv run python -m refdata.area

## Build the GDAL + minisign image the package build runs in. Online, once.
refdata-image: .env
	$(COMPOSE) build refdata-tools

## Create the package signing key: secret key in MINISIGN_KEY (default
## ~/.minisign/dvw.key, never in the repo), public key in refdata/keys/dvw.pub (commit it).
## Refuses to overwrite either. No password, so builds run unattended (ADR 0011).
MINISIGN_KEY ?= $(or $(call env_get,MINISIGN_KEY),$(HOME)/.minisign/dvw.key)
refdata-key: .env
	@test ! -e refdata/keys/dvw.pub || { echo "refdata/keys/dvw.pub exists; delete it first to replace the key" >&2; exit 1; }
	@test ! -e $(MINISIGN_KEY) || { echo "$(MINISIGN_KEY) exists; delete it first to replace the key" >&2; exit 1; }
	mkdir -p $(dir $(MINISIGN_KEY)) refdata/keys
	$(COMPOSE) run --rm --no-deps -v $(dir $(abspath $(MINISIGN_KEY))):/keys -v $(CURDIR)/refdata/keys:/pub \
		refdata-tools minisign -G -W -p /pub/dvw.pub -s /keys/$(notdir $(MINISIGN_KEY))

## Zone package: cut every layer to refdata/area.yaml, sign the manifest, verify it,
## then load it into PostGIS through a staging schema. Offline. A package is
## data/refdata/packages/<version>/; PKG=<dir> verifies or loads another one (rollback).
refdata: refdata-build refdata-verify refdata-load

refdata-build: .env
	uv run python -m refdata build

refdata-verify: .env
	uv run python -m refdata verify $(if $(PKG),--package $(PKG))

refdata-load: .env
	uv run python -m refdata load $(if $(PKG),--package $(PKG))

## Replay the pinned AIS day into ais.raw at SPEED (default 10x, the speed the
## pipeline is sized for). Brokers default to REDPANDA_BROKERS in .env.
## Extra flags via ARGS, e.g. make replay ARGS="--limit 10000"; a --speed in ARGS wins.
SPEED ?= 10
replay: .env
	uv run python -m ingest.replayer --file $(AIS_CSV) --speed $(SPEED) $(ARGS)

## (Re)build and start the ais.raw -> ClickHouse sink.
sink: .env
	$(COMPOSE) up -d --build raw-sink

sink-logs: .env
	$(COMPOSE) logs -f raw-sink

## (Re)build and start ais.alerts -> dvw.alerts and ais.tracks -> dvw.tracks, one process
## (same image as raw-sink). Inserts into dvw.tracks feed the rollups.
tracks-alerts-sink: .env
	$(COMPOSE) up -d --build tracks-alerts-sink

tracks-alerts-sink-logs: .env
	$(COMPOSE) logs -f tracks-alerts-sink

## (Re)build and start the ais.tracks -> PostGIS vessels_current sink (same image).
vessels-sink: .env
	$(COMPOSE) up -d --build vessels-sink

vessels-sink-logs: .env
	$(COMPOSE) logs -f vessels-sink

## (Re)build and start the Faust tracker: ais.enriched -> ais.tracks, ais.alerts.
## TRACKER_INPUT_TOPIC=ais.raw in .env tracks without the Flink job (no DARK_GAP).
tracker: .env
	$(COMPOSE) up -d --build tracker

tracker-logs: .env
	$(COMPOSE) logs -f tracker

## Apply new ClickHouse migrations in sql/clickhouse/ (no-op when up to date).
migrate: .env
	uv run python -m sql.migrate --target clickhouse

## Apply new PostGIS migrations in sql/postgis/ (no-op when up to date).
migrate-postgis: .env
	uv run python -m sql.migrate --target postgis

## Check one replay run against ClickHouse, per partition. Waits up to WAIT seconds
## while rows are only missing (sink catching up). make reconcile RUN=<run_id>
WAIT ?= 30
reconcile: .env
	@test -n "$(RUN)" || { echo "usage: make reconcile RUN=<run_id>  (see data/runs/)" >&2; exit 2; }
	uv run python -m reconcile.run_check $(RUN) --wait $(WAIT)

## Score a --inject run's alerts against its labels: TP/FP/FN per alert type. Waits up
## to WAIT seconds for the tracker and alerts sink to finish the run. make score RUN=<run_id>
score: .env
	@test -n "$(RUN)" || { echo "usage: make score RUN=<run_id>  (a run replayed with --inject)" >&2; exit 2; }
	uv run python -m reconcile.score_alerts $(RUN) --wait $(WAIT)

## Flink session cluster plus the enrichment job: ais.raw -> ais.enriched, ais.dlq,
## ais.dedup_counts (docs/adr/0014). Part of `make up`. Builds the image (which runs the
## Java tests), starts the JobManager and TaskManager, then submits the job unless it is
## already running. The job's startup gate waits for an active zone package in PostGIS
## (docs/adr/0018), so first `refdata ensure` loads the last built package if none is
## active (after `make nuke`; offline, verified). With no package built at all the job is
## not submitted: run `make fetch-refdata refdata`, then `make flink-up`.
## --no-recreate, and --no-deps on the submit (whose `run` would otherwise recreate its
## dependencies): a rebuilt image must not replace a running JobManager, which would drop
## the job without a savepoint (a session cluster keeps no jobs across a restart). Job code
## changes go live with `make flink-restart`, which stops with a savepoint first.
## Web UI: http://localhost:8081
flink-up: .env
	$(COMPOSE) up -d --build --no-recreate --wait --wait-timeout 180 flink-jobmanager flink-taskmanager
	@uv run python -m refdata ensure; rc=$$?; \
	if [ $$rc -eq 3 ]; then \
		echo "flink-up: Flink is running but the job is not submitted: it needs a zone package." >&2; \
		echo "flink-up: run make fetch-refdata refdata, then make flink-up. Until then the tracker gets no input." >&2; \
		exit 0; \
	elif [ $$rc -ne 0 ]; then exit $$rc; fi; \
	$(COMPOSE) run --rm --no-deps flink-submit submit

## Stop the job with a savepoint: the sinks commit their last transactions and the state
## is written to the flink-data volume. The cluster keeps running.
flink-savepoint: .env
	$(COMPOSE) run --rm --no-deps flink-submit stop

## Rebuild the job, stop it with a savepoint if it is running, and start it from the
## latest savepoint. Use after changing the job or AIS_DAY, and after loading a new zone
## package: the restarted job reads whichever package is active (docs/adr/0018).
flink-restart: .env
	$(COMPOSE) build flink-submit
	$(COMPOSE) run --rm --no-deps flink-submit restart

flink-logs: .env
	$(COMPOSE) logs -f flink-jobmanager flink-taskmanager

## Check one replay run through Flink, per partition: raw = enriched + dlq + dedup counts,
## and no raw offset written twice. Waits up to WAIT seconds while records are only missing.
## make flink-check RUN=<run_id>
flink-check: .env
	@test -n "$(RUN)" || { echo "usage: make flink-check RUN=<run_id>  (see data/runs/)" >&2; exit 2; }
	uv run python -m reconcile.flink_check $(RUN) --wait $(WAIT)

## Recompute the zone tags of a sample of one run's ais.enriched records in PostGIS
## (geography distances on the 4326 tables) and report differences. make zone-check RUN=<run_id>
zone-check: .env
	@test -n "$(RUN)" || { echo "usage: make zone-check RUN=<run_id>  (see data/runs/)" >&2; exit 2; }
	uv run python -m reconcile.zone_check $(RUN)

## Compare the ais.tracks of two replays of the same rows, vessel by vessel (e.g. the
## tracker on ais.raw, then on ais.enriched). make compare-tracks A=<run_id> B=<run_id>
compare-tracks: .env
	@test -n "$(A)" -a -n "$(B)" || { echo "usage: make compare-tracks A=<run_id> B=<run_id>" >&2; exit 2; }
	uv run python -m reconcile.compare_tracks $(A) $(B)

## End-to-end latency of one run: replayer send to track sent (every fix) and to
## TELEPORT stored in ClickHouse. make latency RUN=<run_id>
latency: .env
	@test -n "$(RUN)" || { echo "usage: make latency RUN=<run_id>" >&2; exit 2; }
	uv run python -m reconcile.latency $(RUN)

PY_DIRS := common ingest refdata sql sinks reconcile stream

test: test-py test-flink

test-py:
	uv run pytest -q $(PY_DIRS)

## Compile the Flink job and run its unit tests, in Docker (no host JDK or Maven needed).
test-flink:
	docker build --build-context contracts=contracts --target build -q flink/enrichment >/dev/null

typecheck:
	uv run mypy --strict $(PY_DIRS)
