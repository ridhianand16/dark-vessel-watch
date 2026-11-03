-- dvw.alerts: one row per alert on ais.alerts (DARK_GAP, TELEPORT; later MMSI_CLASH).
--
-- The tracker is at-least-once (ADR 0004): after a crash it resends the alerts of the
-- batch it was in, byte-identical, with the same alert_id (ADR 0005). ReplacingMergeTree
-- on alert_id collapses those copies at merge time, so exact counts use FINAL, as on
-- ais_raw. Copies are identical, so which one survives does not matter.
--
-- No PARTITION BY. ReplacingMergeTree only dedupes within a partition, so a partition
-- key must be the same for every copy of an alert. Alerts are a few thousand per day,
-- so one partition is fine for years, and dedup cannot be escaped. Retention comes
-- with the TTL work, in a later migration.
CREATE TABLE IF NOT EXISTS dvw.alerts
(
    -- UUIDv5 from the tracker, or "invalid:<partition>:<offset>" for a record that
    -- could not be decoded (see sinks/clickhouse/alerts.py). String, not UUID, so
    -- that such a record is still stored.
    alert_id        String,
    type            LowCardinality(String),
    mmsi            Nullable(UInt32),
    run_id          LowCardinality(String),
    -- Event time at which the rule became true (not when it was detected).
    ts              Nullable(DateTime64(3, 'UTC')),
    lat             Nullable(Float64),
    lon             Nullable(Float64),
    -- Kalman-predicted position at ts.
    pred_lat        Nullable(Float64),
    pred_lon        Nullable(Float64),
    -- Rule-specific object as JSON text; read with JSONExtractFloat(details, 'gap_s') etc.
    details         String,
    trace_id        String,

    payload         String CODEC(ZSTD(3)),
    decode_error    LowCardinality(String) DEFAULT '',

    -- Of the copy that was stored last; informational, not part of the identity.
    kafka_partition UInt8,
    kafka_offset    UInt64,
    kafka_ts        DateTime64(3, 'UTC'),
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY alert_id;
