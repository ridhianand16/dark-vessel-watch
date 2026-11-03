-- Bookkeeping for sql/migrate.py --target postgis. Each later file runs in one
-- transaction together with its row here.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    integer     PRIMARY KEY,
    name       text        NOT NULL,
    checksum   char(64)    NOT NULL,  -- SHA-256 of the file's bytes
    applied_at timestamptz NOT NULL DEFAULT now()
);
