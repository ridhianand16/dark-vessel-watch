COMPOSE := docker compose -f deploy/compose/docker-compose.yml --env-file .env

.PHONY: hooks up down nuke topics ps fetch replay migrate sink sink-logs tracker tracker-logs reconcile test typecheck

.env:
	@echo "Missing .env. Run: cp .env.example .env" >&2; exit 1

SERVICES := redpanda console clickhouse postgis

# Read only the keys we need from .env rather than including the whole file.
env_get = $(shell sed -n 's/^$(1)=//p' .env 2>/dev/null)
AIS_DAY ?= $(call env_get,AIS_DAY)
AIS_DATA_DIR ?= $(or $(call env_get,AIS_DATA_DIR),./data/ais)
AIS_CSV ?= $(AIS_DATA_DIR)/aisdk-$(AIS_DAY).csv

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
	$(MAKE) migrate
	$(COMPOSE) up -d --build raw-sink tracker

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
	uv run python refdata/fetch.py --source dma_ais --day $(AIS_DAY) --dest $(AIS_DATA_DIR)

## Replay the pinned AIS day into ais.raw. Brokers default to REDPANDA_BROKERS in .env.
## Extra flags via ARGS, e.g. make replay ARGS="--limit 10000 --speed 100"
replay: .env
	uv run python -m ingest.replayer --file $(AIS_CSV) $(ARGS)

## (Re)build and start the ais.raw -> ClickHouse sink.
sink: .env
	$(COMPOSE) up -d --build raw-sink

sink-logs: .env
	$(COMPOSE) logs -f raw-sink

## (Re)build and start the Faust tracker: ais.raw -> ais.tracks.
tracker: .env
	$(COMPOSE) up -d --build tracker

tracker-logs: .env
	$(COMPOSE) logs -f tracker

## Apply new ClickHouse migrations in sql/clickhouse/ (no-op when up to date).
migrate: .env
	uv run python -m sql.migrate

## Check one replay run against ClickHouse, per partition. Waits up to WAIT seconds
## while rows are only missing (sink catching up). make reconcile RUN=<run_id>
WAIT ?= 30
reconcile: .env
	@test -n "$(RUN)" || { echo "usage: make reconcile RUN=<run_id>  (see data/runs/)" >&2; exit 2; }
	uv run python -m reconcile.run_check $(RUN) --wait $(WAIT)

PY_DIRS := common ingest refdata sql sinks reconcile stream

test:
	uv run pytest -q $(PY_DIRS)

typecheck:
	uv run mypy --strict $(PY_DIRS)
