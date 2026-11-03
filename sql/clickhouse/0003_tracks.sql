-- dvw.tracks: one row per accepted fix on ais.tracks (the tracker's Kalman output), the
-- source of the 1-minute and 1-hour rollups (0004).
--
-- Dedup: ORDER BY is the fix's identity. src_partition and src_offset are the fix's
-- ais.raw coordinates, unique per record; mmsi and event_ts come with them and lead the
-- key so one vessel's history is a contiguous range. The tracker's output is a pure
-- function of its state and input, so a track resent after a crash is the same row, and
-- a sink redelivery is too. ReplacingMergeTree collapses copies at merge time; exact
-- counts use FINAL, as on ais_raw and alerts.
--
-- PARTITION BY toDate(event_ts) is safe for dedup because event_ts is part of the
-- identity: every copy of a fix lands in the same partition. A record whose identity
-- cannot be decoded is stored with mmsi = 0 and its own Kafka coordinates and time as
-- the key (sinks/clickhouse/tracks.py), so it is still partitioned the same way on
-- redelivery.
CREATE TABLE IF NOT EXISTS dvw.tracks
(
    mmsi            UInt32,
    event_ts        DateTime64(3, 'UTC'),
    src_partition   UInt8,
    src_offset      UInt64,
    run_id          LowCardinality(String),

    -- Filtered position (Float64: same reason as ais_raw) and velocity.
    lat             Nullable(Float64),
    lon             Nullable(Float64),
    vx_ms           Nullable(Float32),
    vy_ms           Nullable(Float32),
    sog_kn          Nullable(Float32),
    cog_deg         Nullable(Float32),
    pos_sigma_m     Nullable(Float32),
    -- The fix as received.
    meas_lat        Nullable(Float64),
    meas_lon        Nullable(Float64),
    -- Null on a track's first fix and after a reinitialisation (no prediction).
    innovation_m    Nullable(Float32),
    innovation_nis  Nullable(Float32),
    dt_s            Nullable(Float32),
    fix_seq         Nullable(UInt32),
    trace_id        String,
    replay_send_ts  Nullable(DateTime64(3, 'UTC')),

    -- Original bytes, only when decode_error is set.
    payload         String CODEC(ZSTD(3)),
    decode_error    LowCardinality(String) DEFAULT '',

    -- Of the copy that was stored last; informational, not part of the identity.
    kafka_partition UInt8,
    kafka_offset    UInt64,
    kafka_ts        DateTime64(3, 'UTC'),
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(event_ts)
ORDER BY (mmsi, event_ts, src_partition, src_offset);
