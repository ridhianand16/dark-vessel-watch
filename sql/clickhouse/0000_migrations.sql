-- Bookkeeping for sql/migrate.py: one row per applied migration file.
-- checksum is the SHA-256 of the file's bytes. The runner refuses to continue if an
-- applied file has changed: fix forward with a new numbered file instead.
CREATE DATABASE IF NOT EXISTS dvw;

CREATE TABLE IF NOT EXISTS dvw.schema_migrations
(
    version    UInt32,
    name       String,
    checksum   FixedString(64),
    applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY version;
