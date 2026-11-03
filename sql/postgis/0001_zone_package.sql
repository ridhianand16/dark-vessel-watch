-- Live tables for the zone package (refdata/load.py, ADR 0010).
--
-- The loader replaces zones and eez_boundaries on every package load: it creates
-- copies with CREATE TABLE refdata_staging.<t> (LIKE public.<t> INCLUDING ALL), fills
-- and checks them, then drops the live table and moves the copy in, in one
-- transaction. Columns, constraints and indexes defined here therefore carry over to
-- every load; change them with a new migration as usual. Indexes are left unnamed so
-- the copy gets the same default name (zones_geom_idx) and later migrations can rely
-- on it.

-- The postgis image enables this already; the migration does not depend on it.
CREATE EXTENSION IF NOT EXISTS postgis;

-- EEZ polygons, marine protected areas and port circles, clipped to the package area.
CREATE TABLE zones (
    zone_id         text NOT NULL PRIMARY KEY,  -- eez:<MRGID>, wdpa:<SITE_PID>, wpi:<INDEX_NO>
    kind            text NOT NULL CHECK (kind IN ('eez', 'mpa', 'port')),
    name            text NOT NULL,
    source          text NOT NULL,              -- <sources.yaml key>:<version>, e.g. wdpa_marine:Nov2026
    package_version text NOT NULL,
    geom            geometry(MultiPolygon, 4326) NOT NULL
);
CREATE INDEX ON zones USING gist (geom);
CREATE INDEX ON zones (kind);

-- EEZ boundary lines. Distances to a boundary are measured to these, never to the
-- edges of the clipped polygons in zones, which stop at the package bbox.
CREATE TABLE eez_boundaries (
    id              bigint NOT NULL PRIMARY KEY,  -- Marine Regions LINE_ID
    package_version text   NOT NULL,
    geom            geometry(MultiLineString, 4326) NOT NULL
);
CREATE INDEX ON eez_boundaries USING gist (geom);

-- One row per load, including reloads of the same package. The active package is the
-- latest loaded_at.
CREATE TABLE refdata_packages (
    package_version text        NOT NULL,
    manifest_sha256 char(64)    NOT NULL,
    verified_at     timestamptz NOT NULL,
    loaded_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (package_version, loaded_at)
);
