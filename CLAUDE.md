# CLAUDE.md – Dark Vessel Watch

Read PROBLEM_STATEMENT.md first. It is the spec. If a request conflicts with it, say so before coding. Setup details are in docs/SETUP.md.

## Context

Portfolio project for a Data Engineer II role on a streaming ETL platform (Redpanda, Flink, Faust, ClickHouse, PostGIS, self-managed Kubernetes, air-gapped). The engineering quality is the point: correctness, delivery guarantees, reproducibility and docs, not feature count.

I am an experienced backend/data engineer (Python, Java, Scala, Kafka, Spark). I am new to Flink, Faust, ClickHouse and PostGIS. When you introduce something from those, explain the why in two or three lines.

## Stack and versions

- Python 3.12, faust-streaming, rocksdict, numpy, polars, confluent-kafka, clickhouse-connect, psycopg 3, httpx
- Java 17, Flink 1.20 DataStream API, Maven
- Redpanda (Kafka API), ClickHouse 24.8, PostgreSQL 16 + PostGIS 3.4
- Everything runs in Docker Compose (OrbStack) on an 8 GB Apple M1 laptop. Memory is the tightest constraint: give every service a mem_limit and say what it costs.

## Repo layout

```
common/               shared settings (.env reader) and ClickHouse helpers
ingest/replayer/      CSV (and later live AIS) → ais.raw
flink/enrichment/     Java Flink job (not built yet)
stream/faust_app/     Kalman tracking, detections
sinks/                ClickHouse and PostGIS writers
sql/clickhouse/       numbered ClickHouse migrations; sql/migrate.py applies them
sql/postgis/          numbered PostGIS migrations (not built yet)
refdata/              sources.yaml, fetcher; later the package builder and verifier
reconcile/            zero-loss reconciliation job
docs/adr/             Architecture Decision Records (NNNN-title.md)
docs/SETUP.md         setup guide
deploy/compose/       docker-compose.yml
deploy/helm/          charts (later)
.githooks/            commit-msg hook
```

## Rules

- No cloud or managed services, no runtime downloads. The pipeline must run offline once images and data are present. Only `make fetch` goes online.
- Type hints everywhere. mypy --strict must pass on Python code.
- Kafka keys are MMSI. Topics have exactly 6 partitions, and every producer uses the Java-compatible murmur2 partitioner (ADR 0001). Never change partitioning without an ADR.
- Every sink must be idempotent (replay must not create duplicates). Say how in the commit body.
- Reconciliation is per partition over a run's offset range, never a plain total count.
- Every message carries the run_id of the replay that produced it.
- Schema changes are numbered migration files, never edits to existing ones.
- Any non-obvious design choice gets an ADR in docs/adr/: context, decision, alternatives, consequences. Use the next free number.
- Keep each change small and runnable. After a change, give me the exact command to verify it.
- Do not commit data files, .env or archives. Do commit pinned hashes in refdata/sources.yaml.
- In code comments, ADRs and docs, name components ("the Flink validation job", "the reconciliation job"), not milestone codes. The plan changes; the components don't.

## Git workflow

Branching:
- `main` is always runnable. Never commit directly to it.
- One short-lived branch per unit of work, named `<type>/<short-description>` in kebab case, e.g. `feat/detections`, `fix/replayer-pacing-drift`, `docs/adr-raw-sink`.
- Make several small commits on the branch as the work progresses (scaffold, core logic, tests, docs). Each commit builds and passes mypy/tests on its own.
- Merge when the verify checks pass, `make test` and `make typecheck` are green, and the README reflects the change: `git merge --no-ff`, then delete the branch. Start the next branch from the updated `main`, never from an unmerged branch.
- Do not push or open PRs unless I ask.

Commit messages:
- Conventional Commits: `type(scope): subject`. Types: feat, fix, refactor, perf, test, docs, build, chore. Scope is the component: replayer, sink, compose, refdata, tracker, detections, flink, clickhouse, postgis, reconcile, readme, claude.
- Subject: imperative, lower case after the colon, no trailing period, max 72 characters. Say what changed in the system, not what task was done.
- Body (when useful): wrap at 72. Explain why and any trade-off in plain sentences. Mention idempotency or delivery guarantees when relevant. Reference an ADR by number if one applies.
- Never mention milestones, step numbers, plans, prompts, "as requested", or that anything was generated. No `Co-Authored-By`, no `Claude-Session`, no "Generated with" lines, no emoji.
- Avoid filler words: comprehensive, robust, seamless, enhance, leverage, streamline, various, utilize, ensure proper.
- Merge commits: `Merge branch 'feat/detections'` (git default) is fine.

Good:
```
feat(replayer): pace replay by event-time gaps with a speed factor

Sleeps are batched per 100 ms of wall time so 50x replay does not
spend most of its CPU in sleep calls.
```
```
fix(sink): commit offsets only after the ClickHouse insert succeeds
```
Bad: `M1.2: implement replayer as per plan`, `Add comprehensive robust replayer 🚀`, `Update files`.

The `.githooks/commit-msg` hook enforces the format and strips attribution trailers. `make hooks` enables it (and `make up` runs it).

## README

README.md is the front door for a hiring manager, so keep it current. Update it on every branch that changes what the project can do or how to run it.

- Write like a person explaining their own project: first person is fine, plain sentences, concrete numbers. Lead with the problem (ships going dark near protected waters), then what the pipeline does, then how to run it.
- Keep a short "Where it stands" section describing what works today in plain terms, not milestone codes.
- Put measured numbers (throughput, p99 latency, row counts, reconciliation result) in as soon as they exist, with the date, laptop spec and command used.
- Architecture diagram, quickstart (copy-paste commands that work), design decisions linking to ADRs, and known limitations.
- No marketing tone, no badge walls, no emoji headings, no "This project aims to". Short paragraphs. Cut anything a reader would skim past.

## Topics

ais.raw, ais.enriched, ais.tracks, ais.alerts, ais.dlq

## Where we are

Done: local stack; AIS fetch, replayer, raw ClickHouse store and per-run reconciliation; per-vessel Kalman tracking into ais.tracks; DARK_GAP and TELEPORT detections; dvw.alerts via the config-driven ClickHouse sink; labelled anomaly injection and scoring (ADRs 0001 to 0007).

Next: PostGIS zones from a checksummed offline reference package, then the real zone predicate for DARK_GAP, and vessels_current upsert.

Current milestone: M3
