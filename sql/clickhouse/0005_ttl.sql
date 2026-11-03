-- Retention: ais_raw and tracks 7 days, ais_1min 30 days, ais_1hour kept. alerts is
-- small and kept. See docs/adr/0020.
--
-- Measured from ingested_at (when the row was stored), never event_ts. The pipeline
-- replays a past day (2026-10-28): an event-time TTL would delete a replayed row as soon
-- as it arrived, and in live use a late or re-sent record would be dropped on insert.
-- Ingest time says how long the data has been here, which is what retention is about.
--
-- toDateTime(): TTL needs a Date or DateTime; 24.8 rejects DateTime64.
--
-- TTL deletes rows during merges, not on a timer, so expired rows can stay a while
-- (merge_with_ttl_timeout, 4 h by default). A ReplacingMergeTree copy inserted later has
-- a later ingested_at; whichever copy survives the merge sets the row's expiry.
--
-- materialize_ttl_after_modify = 0: by default MODIFY TTL also rewrites every existing
-- part to apply it. On a 690 MiB ais_raw that mutation ran ClickHouse (1.25 GB limit)
-- out of memory during the migration. Without it, new parts get the TTL on insert and
-- old parts when they are next merged. To apply it to old parts at a quiet time:
--   ALTER TABLE dvw.ais_raw MATERIALIZE TTL
-- Re-running these statements is harmless.

ALTER TABLE dvw.ais_raw MODIFY TTL toDateTime(ingested_at) + INTERVAL 7 DAY
SETTINGS materialize_ttl_after_modify = 0;

ALTER TABLE dvw.tracks MODIFY TTL toDateTime(ingested_at) + INTERVAL 7 DAY
SETTINGS materialize_ttl_after_modify = 0;

-- ingested_at here is the latest insert into the bucket (SimpleAggregateFunction max).
ALTER TABLE dvw.ais_1min MODIFY TTL toDateTime(ingested_at) + INTERVAL 30 DAY
SETTINGS materialize_ttl_after_modify = 0;
