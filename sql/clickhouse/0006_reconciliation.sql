-- dvw.reconciliation: the reconciliation job's verdicts (reconcile/job.py), one row per
-- run, ais.raw partition and check. See docs/adr/0021.
--
-- ReplacingMergeTree(checked_at): rerunning the job for a run writes the same keys with
-- a later checked_at, and the merge keeps only the newest row per key. Read with FINAL
-- to see only the latest verdict before that merge has happened.
--
-- partition is the ais.raw partition (0-5); -1 is a run-wide row (the latency check).
--
-- expected and actual are the two sides of the check (for raw_stored: produced and
-- stored). The count columns repeat the partition's full numbers on every row of that
-- partition, so a dashboard can read any one row on its own. On the run-wide row they
-- are the run's totals.
CREATE TABLE IF NOT EXISTS dvw.reconciliation
(
    run_id          LowCardinality(String),
    partition       Int16,
    check           LowCardinality(String),
    status          Enum8('ok' = 1, 'warn' = 2, 'fail' = 3),
    note            String,
    expected        UInt64,
    actual          UInt64,

    -- Counts over the run's offset range of this ais.raw partition.
    produced        UInt64,   -- acknowledged by the broker (produced.json)
    stored          UInt64,   -- rows in dvw.ais_raw FINAL
    enriched        UInt64,   -- ais.enriched records, read_committed
    dlq             UInt64,   -- ais.dlq records
    duplicates      UInt64,   -- sum of ais.dedup_counts.count
    untagged        UInt64,   -- enriched records without zone tags
    tracks          UInt64,   -- rows in dvw.tracks FINAL (decoded)
    rollup_fixes    UInt64,   -- sum of n_fixes in dvw.ais_1min_v
    -- Data quality: what the Flink job removed, and why.
    dlq_reasons     Map(LowCardinality(String), UInt64),
    dlq_share       Float64,  -- dlq / produced

    -- Only on the latency row: replay_send_ts to first stored in dvw.tracks, in ms.
    latency_n       UInt64,
    latency_p50_ms  Nullable(Float64),
    latency_p95_ms  Nullable(Float64),
    latency_p99_ms  Nullable(Float64),

    checked_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(checked_at)
ORDER BY (run_id, partition, check);
