# Design decisions

Each decision has an Architecture Decision Record in [adr/](adr/) with its context, the alternatives and the consequences.

| ADR | Decision |
|---|---|
| [0001](adr/0001-kafka-partitioner-murmur2.md) | Every producer partitions by murmur2(MMSI), the Java client's hash, so a vessel's records keep one partition number on every topic. |
| [0002](adr/0002-replayer-ordering-and-raw-contract.md) | The replayer sorts raw CSV lines by event time and sends every row, with the original line, in the `ais.raw` v1 contract. |
| [0003](adr/0003-raw-sink-python-consumer.md) | The raw sink is a Python consumer that commits offsets after the insert, into a `ReplacingMergeTree` keyed on partition and offset. |
| [0004](adr/0004-tracker-at-least-once-outputs-before-state.md) | The tracker is at-least-once: outputs are acknowledged before state is written, and every sink is idempotent. |
| [0005](adr/0005-detections-in-the-tracker-event-time.md) | Detections run in the tracker, on event time, with one deterministic `alert_id` per event. |
| [0006](adr/0006-alerts-sink-config-driven.md) | The alerts sink is the raw sink driven by a spec, writing to a table keyed by `alert_id`. |
| [0007](adr/0007-injected-anomalies-and-scoring.md) | Labelled synthetic vessels are injected into the replay and scored by exact `alert_id`. |
| [0008](adr/0008-10x-replay-target-and-ship-only-tracking.md) | The pipeline is sized for 10x replay on one worker per service, and tracks only ship stations. |
| [0009](adr/0009-zone-sources-pinned-and-area-from-the-feed.md) | Zone sources are pinned files, and the package covers only the feed's area. |
| [0010](adr/0010-zone-package-staging-swap-and-port-radius.md) | Zone packages are swapped in from a checked staging schema; ports get one fixed 2 nm radius. |
| [0011](adr/0011-signed-zone-package-built-from-pinned-sources.md) | The zone package is a minisign-signed manifest over per-layer GeoPackages, built locally from pinned sources and never published. |
| [0012](adr/0012-vessels-current-upsert-forward-in-event-time.md) | `vessels_current` is an upsert per MMSI that only moves forward in event time. |
| [0013](adr/0013-metric-zone-copy-and-active-package.md) | The loader writes EPSG:25832 copies of the zone layers, and one package is explicitly active. |
| [0014](adr/0014-flink-exactly-once-3s-checkpoints-5s-latency.md) | The Flink enrichment job is exactly-once with 3 s checkpoints, which sets the end-to-end target at p99 under 5 s. |
| [0015](adr/0015-dedup-key-and-counted-duplicates.md) | Dedup keys on run, MMSI, event time and rounded position, and counts what it drops so raw = enriched + dlq + duplicates. |
| [0016](adr/0016-dedup-counts-topic.md) | Dropped duplicates are counted into their own topic, `ais.dedup_counts`. |
| [0017](adr/0017-zone-pieces-for-in-memory-lookup.md) | Zones are cut into pieces of at most 256 vertices for in-memory lookup. |
| [0018](adr/0018-flink-zone-tagging-read-at-startup.md) | Flink reads the zones at startup into one shared index per TaskManager, and buffers records until they are loaded. |
| [0019](adr/0019-tracker-on-enriched-and-dark-gap-zone-condition.md) | The tracker reads `ais.enriched` with `read_committed`, and DARK_GAP's zone condition reads Flink's tags. |
| [0020](adr/0020-rollups-from-tracks-and-ttl.md) | Rollups use only aggregates a repeated track cannot change, and TTL is measured from `ingested_at`. |
| [0021](adr/0021-reconciliation-job.md) | One reconciliation job per run waits on committed offsets, checks every stage per partition and stores the verdict. |
| [0022](adr/0022-flink-restores-newest-retained-state.md) | Every Flink job start restores the newest savepoint or retained checkpoint; fresh only when there is none. |
| [0023](adr/0023-results-report-one-command.md) | `make report` writes a dated results report from one 10x run; `CHAOS=1` kills the TaskManager, tracker and JobManager (SIGKILL, so the checkpoint survives). |
