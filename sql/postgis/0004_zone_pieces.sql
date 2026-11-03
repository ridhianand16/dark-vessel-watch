-- The metric zone layers cut into small pieces, for the Flink enrichment job (ADR 0017).
--
-- The loader (refdata/load.py) fills these from zones_25832 and eez_boundaries_25832
-- with ST_Subdivide, at most 256 vertices per piece, and checks that the pieces of
-- each zone have the zone's area and the pieces of each boundary its length. The
-- Flink job keeps them in memory in an STRtree: a lookup finds the few pieces whose
-- box holds the point and scans at most 256 vertices each, where a whole protected
-- area can have nearly a million.
--
-- Like the other zone tables they are replaced on every load through the staging
-- swap, so indexes stay unnamed and carry over.

CREATE TABLE zone_pieces_25832 (
    zone_id         text NOT NULL,
    piece           integer NOT NULL,  -- 1..n within the zone, in ST_Subdivide order
    kind            text NOT NULL CHECK (kind IN ('eez', 'mpa', 'port')),
    package_version text NOT NULL,
    geom            geometry(Polygon, 25832) NOT NULL,
    PRIMARY KEY (zone_id, piece)
);
CREATE INDEX ON zone_pieces_25832 USING gist (geom);

CREATE TABLE eez_boundary_pieces_25832 (
    id              bigint  NOT NULL,
    piece           integer NOT NULL,
    package_version text    NOT NULL,
    geom            geometry(LineString, 25832) NOT NULL,
    PRIMARY KEY (id, piece)
);
CREATE INDEX ON eez_boundary_pieces_25832 USING gist (geom);
