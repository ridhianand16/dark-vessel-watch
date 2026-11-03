-- Latest filtered position per vessel, upserted from ais.tracks by the vessels_current
-- sink (sinks/postgis_vessels, ADR 0012). A row only moves forward in event time: an
-- upsert replaces it only when its event_ts is strictly later, so a replayed or
-- reprocessed track message writes nothing.

CREATE TABLE vessels_current (
    mmsi       integer     NOT NULL PRIMARY KEY CHECK (mmsi BETWEEN 0 AND 999999999),
    run_id     text        NOT NULL,  -- the replay run of the track message stored here
    event_ts   timestamptz NOT NULL,  -- event time of that fix
    position   geometry(Point, 4326) NOT NULL,  -- Kalman-filtered, not the fix as received
    sog        double precision NOT NULL,  -- knots, filtered
    cog        double precision NOT NULL,  -- degrees, filtered (0 when stationary)
    updated_at timestamptz NOT NULL DEFAULT now()  -- wall time of the transaction that wrote the row
);
-- For "which vessels are in this zone" (map, DARK_GAP zone check).
CREATE INDEX ON vessels_current USING gist (position);
