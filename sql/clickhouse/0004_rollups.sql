-- dvw.ais_1min and dvw.ais_1hour: per-vessel downsampled history, built from dvw.tracks
-- (ships only, Kalman-filtered). ais_raw keeps every record unchanged; these are for
-- reading a vessel's day at a glance. See docs/adr/0020.
--
-- Why only these aggregates. A materialised view is an insert trigger: it runs on each
-- block inserted into dvw.tracks, before ReplacingMergeTree has merged anything. When the
-- sink re-inserts a batch after a crash, or the tracker resends tracks, the view sees
-- the copies and adds them to the rollup a second time; the later merge of dvw.tracks
-- does not reach the rollup. So every aggregate here gives the same result whether a
-- fix is counted once or twice:
--   last position, speed, course   argMax by (event_ts, src_offset): the same fix wins
--                                  either way; src_offset breaks event_ts ties so raw,
--                                  1-minute and 1-hour agree on which fix is "last"
--   min/max speed, max innovation  a repeated value does not move a min or max
--   n_fixes                        uniqExact over the fix's identity (src_partition,
--                                  src_offset), not count()
-- No count(), sum() or avg(): those would grow with every copy.
--
-- The same property makes the backfill (make backfill-rollups) safe to run any number
-- of times, also while the sink is inserting.
--
-- Both rollups read dvw.tracks directly rather than 1-hour from 1-minute: one view per
-- table, each a plain GROUP BY over tracks, with nothing to keep in step between them.
--
-- ORDER BY (run_id, mmsi, bucket): a replay of the same day again has the same event
-- times but new offsets, so without run_id two runs would share buckets and n_fixes
-- would add up across them. In live use run_id barely changes and the key reads as
-- (mmsi, bucket). No PARTITION BY: rows are small (one per vessel per bucket) and the
-- 1-minute TTL (0005) is per row.
--
-- ingested_at is the latest insert into the bucket. It is not a data aggregate (a
-- resend moves it); it only drives retention, so a bucket lives 30 days from its last
-- write.

CREATE TABLE IF NOT EXISTS dvw.ais_1min
(
    run_id           LowCardinality(String),
    mmsi             UInt32,
    bucket           DateTime('UTC'),
    -- (lat, lon, sog_kn, cog_deg) of the last fix. One state for the four, not four
    -- states: measured on a 413k-fix run, merging six parts took 147 MiB instead
    -- of 252 MiB, and the 1-minute GROUP BY ran 3.6 times faster.
    last_fix         AggregateFunction(argMax, Tuple(Float64, Float64, Float32, Float32), Tuple(DateTime64(3, 'UTC'), UInt64)),
    last_event_ts    SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    min_sog_kn       SimpleAggregateFunction(min, Float32),
    max_sog_kn       SimpleAggregateFunction(max, Float32),
    max_innovation_m SimpleAggregateFunction(max, Nullable(Float32)),
    n_fixes          AggregateFunction(uniqExact, Tuple(UInt8, UInt64)),
    ingested_at      SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (run_id, mmsi, bucket);

CREATE TABLE IF NOT EXISTS dvw.ais_1hour AS dvw.ais_1min
ENGINE = AggregatingMergeTree
ORDER BY (run_id, mmsi, bucket);

-- decode_error = '' keeps out rows the sink could not decode (mmsi = 0, null
-- positions). Every decoded track has lat, lon, sog_kn and cog_deg, so assumeNotNull
-- only changes the type.
CREATE MATERIALIZED VIEW IF NOT EXISTS dvw.ais_1min_mv TO dvw.ais_1min AS
SELECT
    run_id,
    mmsi,
    toStartOfMinute(event_ts) AS bucket,
    argMaxState(
        (assumeNotNull(lat), assumeNotNull(lon), assumeNotNull(sog_kn), assumeNotNull(cog_deg)),
        (event_ts, src_offset)
    ) AS last_fix,
    max(event_ts) AS last_event_ts,
    min(assumeNotNull(sog_kn)) AS min_sog_kn,
    max(assumeNotNull(sog_kn)) AS max_sog_kn,
    max(innovation_m) AS max_innovation_m,
    uniqExactState((src_partition, src_offset)) AS n_fixes,
    max(ingested_at) AS ingested_at
FROM dvw.tracks
WHERE decode_error = ''
GROUP BY run_id, mmsi, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS dvw.ais_1hour_mv TO dvw.ais_1hour AS
SELECT
    run_id,
    mmsi,
    toStartOfHour(event_ts) AS bucket,
    argMaxState(
        (assumeNotNull(lat), assumeNotNull(lon), assumeNotNull(sog_kn), assumeNotNull(cog_deg)),
        (event_ts, src_offset)
    ) AS last_fix,
    max(event_ts) AS last_event_ts,
    min(assumeNotNull(sog_kn)) AS min_sog_kn,
    max(assumeNotNull(sog_kn)) AS max_sog_kn,
    max(innovation_m) AS max_innovation_m,
    uniqExactState((src_partition, src_offset)) AS n_fixes,
    max(ingested_at) AS ingested_at
FROM dvw.tracks
WHERE decode_error = ''
GROUP BY run_id, mmsi, bucket;

-- Read side: the -Merge combinators finish the partial states. Query these, not the
-- tables: a bucket can be spread over several unmerged rows until a background merge.
CREATE VIEW IF NOT EXISTS dvw.ais_1min_v AS
SELECT
    run_id,
    mmsi,
    bucket,
    argMaxMerge(last_fix) AS last,
    last.1 AS lat,
    last.2 AS lon,
    last.3 AS sog_kn,
    last.4 AS cog_deg,
    max(last_event_ts) AS last_event_ts,
    min(min_sog_kn) AS min_sog_kn,
    max(max_sog_kn) AS max_sog_kn,
    max(max_innovation_m) AS max_innovation_m,
    uniqExactMerge(n_fixes) AS n_fixes
FROM dvw.ais_1min
GROUP BY run_id, mmsi, bucket;

CREATE VIEW IF NOT EXISTS dvw.ais_1hour_v AS
SELECT
    run_id,
    mmsi,
    bucket,
    argMaxMerge(last_fix) AS last,
    last.1 AS lat,
    last.2 AS lon,
    last.3 AS sog_kn,
    last.4 AS cog_deg,
    max(last_event_ts) AS last_event_ts,
    min(min_sog_kn) AS min_sog_kn,
    max(max_sog_kn) AS max_sog_kn,
    max(max_innovation_m) AS max_innovation_m,
    uniqExactMerge(n_fixes) AS n_fixes
FROM dvw.ais_1hour
GROUP BY run_id, mmsi, bucket;
