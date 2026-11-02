COMPOSE := docker compose -f deploy/compose/docker-compose.yml --env-file .env

.PHONY: hooks up down nuke topics ps fetch replay test typecheck

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

test:
	uv run pytest -q ingest/

typecheck:
	uv run mypy --strict ingest/ refdata/
