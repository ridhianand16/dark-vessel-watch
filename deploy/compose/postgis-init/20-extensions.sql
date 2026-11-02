-- Runs once, on first start with an empty data volume.
-- The postgis image already enables the extension in POSTGRES_DB; this keeps
-- it explicit and idempotent so the stack does not depend on image internals.
CREATE EXTENSION IF NOT EXISTS postgis;
