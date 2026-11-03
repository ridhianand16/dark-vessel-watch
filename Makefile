COMPOSE := docker compose -f deploy/compose/docker-compose.yml --env-file .env

.PHONY: hooks up down nuke topics ps fetch fetch-refdata area refdata refdata-image refdata-key refdata-build refdata-verify refdata-load replay migrate migrate-postgis sink sink-logs alerts-sink alerts-sink-logs vessels-sink vessels-sink-logs tracker tracker-logs reconcile score test typecheck

.env:
	@echo "Missing .env. Run: cp .env.example .env" >&2; exit 1

SERVICES := redpanda console clickhouse postgis

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

## Start the stack, wait until every service is healthy, then create topics.
## init-topics is run separately: `up --wait` treats a one-shot container that
## exits (even with 0) as a failure.
up: hooks .env
	$(COMPOSE) up -d --wait --wait-timeout 180 $(SERVICES)
	$(COMPOSE) run --rm init-topics
	$(MAKE) migrate migrate-postgis
	$(COMPOSE) up -d --build raw-sink alerts-sink vessels-sink tracker

## Stop and remove containers; named volumes (topic data, DBs) are kept.
down: .env
	$(COMPOSE) down

## Stop and delete everything including volumes. Destroys all data.
nuke: .env
	$(COMPOSE) down -v --remove-orphans

## (Re)create topics. Idempotent: safe to run any number of times.
topics: .env
	$(COMPOSE) run --rm init-topics

ps: .env
	$(COMPOSE) ps -a

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

## (Re)build and start the ais.alerts -> ClickHouse sink (same image as raw-sink).
alerts-sink: .env
	$(COMPOSE) up -d --build alerts-sink

alerts-sink-logs: .env
	$(COMPOSE) logs -f alerts-sink

## (Re)build and start the ais.tracks -> PostGIS vessels_current sink (same image).
vessels-sink: .env
	$(COMPOSE) up -d --build vessels-sink

vessels-sink-logs: .env
	$(COMPOSE) logs -f vessels-sink

## (Re)build and start the Faust tracker: ais.raw -> ais.tracks.
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

PY_DIRS := common ingest refdata sql sinks reconcile stream

test:
	uv run pytest -q $(PY_DIRS)

typecheck:
	uv run mypy --strict $(PY_DIRS)
