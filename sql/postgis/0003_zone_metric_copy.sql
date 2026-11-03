-- Metric copies of the zone layers and an explicit active-package marker (ADR 0013).
--
-- zones_25832 and eez_boundaries_25832 hold the same rows as zones and eez_boundaries,
-- reprojected to EPSG:25832 (ETRS89 / UTM zone 32N, metres) by the loader
-- (refdata/load.py) after the 4326 layers are repaired with ST_MakeValid. The Flink
-- enrichment job reads these and measures distances in metres with plain planar
-- geometry, so it never cleans or reprojects zones itself. Like the 4326 tables they
-- are replaced on every load through the staging swap, so indexes stay unnamed here
-- (zones_25832_geom_idx) and carry over.
--
-- There are no foreign keys to the 4326 tables: the swap drops and replaces both
-- sides in one transaction, and a key would block the DROP.

CREATE TABLE zones_25832 (
    zone_id         text NOT NULL PRIMARY KEY,
    kind            text NOT NULL CHECK (kind IN ('eez', 'mpa', 'port')),
    name            text NOT NULL,
    source          text NOT NULL,
    package_version text NOT NULL,
    geom            geometry(MultiPolygon, 25832) NOT NULL
);
CREATE INDEX ON zones_25832 USING gist (geom);
CREATE INDEX ON zones_25832 (kind);

CREATE TABLE eez_boundaries_25832 (
    id              bigint NOT NULL PRIMARY KEY,
    package_version text   NOT NULL,
    geom            geometry(MultiLineString, 25832) NOT NULL
);
CREATE INDEX ON eez_boundaries_25832 USING gist (geom);

-- The loader's swap clears the old marker and inserts the new load with active = true,
-- in the same transaction as the table swap. The partial unique index allows at most
-- one active row. The active package version:
--
--     SELECT package_version FROM refdata_packages WHERE active;
--
-- Earlier loads stay inactive: they wrote no metric copies, so no package is active
-- until the next refdata load.
ALTER TABLE refdata_packages ADD COLUMN active boolean NOT NULL DEFAULT false;
CREATE UNIQUE INDEX refdata_packages_one_active ON refdata_packages (active) WHERE active;
