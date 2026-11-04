# 0021. One reconciliation job per run: wait on committed offsets, store the verdict

Status: accepted (2026-11-04)

## Context

The pipeline's zero-loss claim was spread over separate tools: `run_check` (produced
= stored in `ais_raw`), `flink_check` (raw = enriched + dlq + duplicates, zone tags),
plus manual checks of the rollups and the tracker counters. Each one guessed when the
pipeline had finished a run, with a `--wait` that only re-checked while counts were
short. None of them kept its result, so a dashboard or results report could not show
that a run balanced.

A run's records pass six consumer groups: the raw sink and the Flink job on `ais.raw`,
the tracker on `ais.enriched`, and the tracks, vessels and alerts sinks on `ais.tracks`
and `ais.alerts`. Counting before all of them have finished gives a false shortfall.
Counting after a fixed sleep is either slow or wrong.

## Decision

1. **One job, one command:** `make reconcile-run RUN=<id>` (`reconcile/job.py`).
   `make reconcile` and `make flink-check` are removed. `run_check` and `flink_check`
   stay as the libraries behind its checks. Alert scoring, the zone-tag sample against
   PostGIS and the latency report answer other questions and keep their own targets.
2. **Run state from the replayer.** The replayer writes `produced.json` with status
   `running` when it starts and overwrites it at the end. The job waits while it says
   `running`, and refuses a missing file or any other status that is not `complete`.
3. **"Done" means committed offsets, stage by stage.** The job waits until each group
   has committed past the run's last record on its input topic:

   | Topic | Groups | Run's last record |
   |---|---|---|
   | `ais.raw` | `dvw-raw-sink`, `dvw-flink-enrichment` | from `produced.json` |
   | `ais.enriched` | `dvw-tracker` | read (committed) once Flink is past the run |
   | `ais.tracks` | `dvw-tracks-sink`, `dvw-vessels-sink` | read once the tracker is past it |
   | `ais.alerts` | `dvw-alerts-sink` | read once the tracker is past it |

   The target is the run's last *record*, never the end of the log. Flink's
   transaction markers occupy the last offsets of `ais.enriched`, and a consumer
   cannot commit past a marker until another record arrives. A target of
   "log end" would wait forever on an idle pipeline. The tracker sends a record's outputs
   before it commits the record (ADR 0004), so once it is past the run, every track and
   alert of the run is on its topic.
4. **Checks per `ais.raw` partition over the run's offset range:** `raw_stored`,
   `flink_balance`, `zone_tagged`, `rollup` (`sum(n_fixes)` of `ais_1min_v` =
   `count() FINAL` of `dvw.tracks`), `dlq_share`. Run-wide: `latency`. With a baseline
   run of the same rows: `tracks_match`, vessel by vessel (`compare_tracks`). A vessel's
   rollup is attributed to the partition of its tracks: key = MMSI, so all of one
   vessel's records share a partition (ADR 0001).
5. **Fail, warn, ok.** Any imbalance fails and makes the exit code 1. A DLQ share above
   5% (`--dlq-warn`) and a p99 above 5 s (`--latency-warn-ms`) only warn. Both are
   data-quality and performance signals, not loss.
6. **Verdicts in `dvw.reconciliation`** (migration 0006): one row per (run, partition,
   check), `ReplacingMergeTree(checked_at)` ordered by `(run_id, partition, check)`.
   A rerun replaces the earlier verdict. Every row carries its partition's counts and
   DLQ reasons. The latency row (partition -1) carries p50/p95/p99. If the pipeline
   does not finish within `--timeout`, nothing is written: a timeout says nothing
   about loss.
7. **Latency is replay send to the fix's first insert into `dvw.tracks`**,
   `min(ingested_at)` per fix without FINAL, so a re-inserted batch does not count from
   its retry.
8. **A short settle.** Flink commits its source offsets and its sink transactions in
   separate callbacks after a checkpoint, so the last transaction can become visible a
   moment after the offsets. While any check fails *and* every count is only short,
   the job re-checks for up to 30 s (`--settle`).

## Alternatives

- **Keep the separate tools and add a wrapper.** Five commands with five `--wait`
  heuristics. A wrapper would still guess when each stage was done.
- **Wait on consumer lag reaching zero.** Zero lag is never reached on `ais.enriched`
  (transaction markers, above), and on a busy pipeline it waits for unrelated runs.
- **Wait for a fixed time after the replay.** Wrong under backlog (the tracker took
  50 s after a 2x replay of 10,000 rows), slow when idle.
- **A plain table, one row per run.** Totals hide a partition that lost exactly as
  many records as another gained. The project rule is per partition.
- **Check the tracker's counters here too** (read = sum of skip reasons and results).
  They are logged, not stored per run, so the job cannot read them per run yet. The
  `rollup` check covers what the tracker emitted, not what it skipped.

## Consequences

- A clean 10,000-row run reconciles in about 33 s, most of it waiting for the tracker.
  The job reads the run's `ais.enriched`, `ais.dlq`, `ais.dedup_counts` and
  `ais.tracks` records from Kafka. On a full day that is several million JSON
  messages and takes minutes.
- The job depends on the consumer group names in `sinks/clickhouse/specs.py`,
  `sinks/postgis_vessels/sink.py`, `EnrichmentJob.GROUP_ID` and the tracker. Renaming
  one without the job makes it wait until `--timeout`.
- A replayer killed with SIGKILL leaves `running` behind. The job then times out (exit 2)
  rather than refusing at once.
- `ais_raw` and `tracks` expire after 7 days (ADR 0020), so a run can only be
  reconciled again within that window. Its stored verdict stays.
- The tracks comparison is only meaningful against a run of the same rows. It reads both
  runs' tracks from Kafka.

## Note (2026-11-04): how the reconciliation readers consume

Every reconciliation reader goes through `reconcile/kafka.py` and consumes with
`read_committed`, so it never counts records from aborted Flink transactions. Each one
uses `auto.offset.reset=earliest`, and a missing topic raises `RunError`.
