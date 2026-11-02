COMPOSE := docker compose -f deploy/compose/docker-compose.yml --env-file .env

.PHONY: up down nuke topics ps

.env:
	@echo "Missing .env. Run: cp .env.example .env" >&2; exit 1

SERVICES := redpanda console clickhouse postgis

## Start the stack, wait until every service is healthy, then create topics.
## init-topics is run separately: `up --wait` treats a one-shot container that
## exits (even with 0) as a failure.
up: .env
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
