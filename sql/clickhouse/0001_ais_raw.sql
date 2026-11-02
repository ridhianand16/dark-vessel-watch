-- dvw.ais_raw: every message on ais.raw, one row per Kafka record. Nothing is dropped:
-- a value that is not valid JSON or does not fit its column still gets a row, with the
-- original bytes in payload and a reason code in decode_error.
--
-- Dedup happens at merge time: ReplacingMergeTree collapses rows with the same ORDER BY
-- key only when background merges combine parts, at no guaranteed time. A batch the sink
-- re-inserts after a crash is stored twice until then, so reads that need exact counts
-- (reconciliation) use FINAL, which applies the collapse at query time. See docs/adr/0003.
CREATE TABLE IF NOT EXISTS dvw.ais_raw
(
    run_id          LowCardinality(String),
    event_ts        Nullable(DateTime64(3, 'UTC')),
    replay_send_ts  Nullable(DateTime64(3, 'UTC')),

    -- Widths follow the AIS message layout, so an out-of-range value is flagged in
    -- decode_error rather than stored: MMSI and IMO are 30-bit, heading is 0-359 or 511,
    -- length is two 9-bit fields (max 1022), width two 6-bit fields (max 126).
    mmsi            Nullable(UInt32),
    -- Float64 keeps the source digits exactly (Float32 would round 56.610395).
    lat             Nullable(Float64),
    lon             Nullable(Float64),
    -- One decimal in the source; Float32 is exact enough.
    sog             Nullable(Float32),
    cog             Nullable(Float32),
    heading         Nullable(UInt16),
    nav_status      LowCardinality(Nullable(String)),
    ship_type       LowCardinality(Nullable(String)),
    name            Nullable(String),
    imo             Nullable(UInt32),
    callsign        Nullable(String),
    length          Nullable(UInt16),
    width           Nullable(UInt8),

    raw             String CODEC(ZSTD(3)),
    payload         String CODEC(ZSTD(3)),
    decode_error    LowCardinality(String) DEFAULT '',

    kafka_partition UInt8,
    kafka_offset    UInt64,
    kafka_ts        DateTime64(3, 'UTC'),
    trace_id        String,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
-- The partition expression may only use fields that are identical in every delivery
-- of the same Kafka record (payload fields, Kafka metadata). ReplacingMergeTree only
-- dedupes within a partition: with ingested_at here, a redelivery on a later day would
-- land in another partition and its duplicate would never be merged away.
PARTITION BY toDate(ifNull(event_ts, kafka_ts))
ORDER BY (kafka_partition, kafka_offset);
