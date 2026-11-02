# CLAUDE.md – Dark Vessel Watch

Read PROBLEM_STATEMENT.md first. It is the spec. If a request conflicts with it, say so before coding.

## Context

Portfolio project for a Data Engineer II role on a streaming ETL platform (Redpanda, Flink, Faust, ClickHouse, PostGIS, self-managed Kubernetes, air-gapped). The engineering quality is the point: correctness, delivery guarantees, reproducibility and docs, not feature count.

I am an experienced backend/data engineer (Python, Java, Scala, Kafka, Spark). I am new to Flink, Faust, ClickHouse and PostGIS. When you introduce something from those, explain the why in two or three lines.

## Stack and versions

- Python 3.12, faust-streaming, rocksdict, numpy, polars, clickhouse-connect, psycopg 3
- Java 17, Flink 1.20 DataStream API, Maven
- Redpanda (Kafka API), ClickHouse 24.x, PostgreSQL 16 + PostGIS 3.4
- Everything runs in Docker Compose on a laptop with about 8 GB RAM to spare

## Repo layout

```
ingest/replayer/      CSV and live AIS → ais.raw
flink/enrichment/     Java Flink job
stream/faust_app/     Kalman tracking, detections
sinks/                ClickHouse and PostGIS writers
sql/clickhouse/       DDL, MVs, TTL (numbered migrations)
sql/postgis/          DDL, migrations
refdata/              offline reference package builder and verifier
reconcile/            zero-loss reconciliation job
docs/adr/             Architecture Decision Records (NNNN-title.md)
deploy/compose/       docker-compose.yml
deploy/helm/          charts (later)
```

## Rules

- No cloud or managed services, no runtime downloads. The pipeline must run offline once images and data are present.
- Type hints everywhere. mypy --strict must pass on Python code.
- Kafka keys are MMSI. Never change partitioning without an ADR.
- Every sink must be idempotent (replay must not create duplicates). State how in the PR or commit.
- Schema changes are numbered migration files, never edits to existing ones.
- Any non-obvious design choice gets an ADR in docs/adr/: context, decision, alternatives, consequences.
- Keep each change small and runnable. After a change, give me the exact command to verify it.
- Do not commit data files. Put download steps in a script under scripts/.

## Topics

ais.raw, ais.enriched, ais.tracks, ais.alerts, ais.dlq

## Current milestone

M1
