"""Check the Flink job's zone tags for one replay run against PostGIS.

    uv run python -m reconcile.zone_check <run_id> [--sample 400]   # or: make zone-check RUN=<run_id>

Reads the run's ais.enriched records (read_committed), takes a sample: every record the job
put in a protected area or port (up to half the sample), the rest at random. For each, it
recomputes the tags in PostGIS on the EPSG:4326 tables the investigators use, independently
of the job's EPSG:25832 pieces (docs/adr/0018):

    eez_id               smallest zone_id of the EEZs that intersect the point
    mpa_ids              protected areas that intersect the point
    in_port              any port circle intersects the point
    dist_eez_boundary_m  ST_Distance(point::geography, eez_boundaries.geom::geography), minimum
    dist_mpa_m           0 inside a protected area, else the same to the nearest one, with
                         its edges densified to 0.001 degrees first

and reports disagreements and the largest distance differences. A tag differs only for points
within centimetres of an edge, where the two tables' edges differ by the densification.

Why densify the protected areas: a polygon edge in the package is straight in degrees, as
ST_Contains reads it and as the metric copy keeps it (ADR 0013). `::geography` instead reads
an edge as a geodesic, and over the long straight edges of offshore sites and of the bbox
clip the two lines are tens of metres apart (28 m in the first run). Short segments make the
two readings agree. Boundary lines need nothing: the loader densified them along the
geodesic already.

Exit codes: 0 every tag agrees and distances are within --tolerance metres, 1 otherwise,
2 the run cannot be checked.
"""

from __future__ import annotations

import argparse
import random
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

from common.env import REPO_ROOT, setting
from common.postgis import conninfo
from reconcile.flink_check import ENRICHED
from reconcile.kafka import read_topic, run_started_ms
from reconcile.run_check import RunError

# One row per sampled point. Nearest protected area: the 5 nearest in the metric copy by
# bounding-box distance (KNN on the GiST index), then the exact geodesic minimum over those.
QUERY = """
WITH pts AS (
    SELECT i, ST_SetSRID(ST_Point(lon, lat), 4326) AS g
    FROM unnest(%(lon)s::float8[], %(lat)s::float8[]) WITH ORDINALITY AS t(lon, lat, i)
)
SELECT
    i,
    (SELECT min(zone_id) FROM zones WHERE kind = 'eez' AND ST_Intersects(geom, g)),
    (SELECT coalesce(array_agg(zone_id ORDER BY zone_id), '{}') FROM zones
      WHERE kind = 'mpa' AND ST_Intersects(geom, g)),
    EXISTS (SELECT 1 FROM zones WHERE kind = 'port' AND ST_Intersects(geom, g)),
    (SELECT min(ST_Distance(g::geography, geom::geography)) FROM eez_boundaries),
    (SELECT min(ST_Distance(g::geography, ST_Segmentize(z.geom, 0.001)::geography))
       FROM (SELECT zone_id FROM zones_25832 WHERE kind = 'mpa'
             ORDER BY geom <-> ST_Transform(g, 25832) LIMIT 5) n
       JOIN zones z USING (zone_id))
FROM pts ORDER BY i
"""


@dataclass
class Result:
    checked: int = 0
    eez: int = 0
    mpa: int = 0
    port: int = 0
    max_boundary_m: float = 0.0
    max_mpa_m: float = 0.0
    eez_ids: Counter[str] = field(default_factory=Counter)
    in_mpa: int = 0
    in_port: int = 0
    examples: list[str] = field(default_factory=list)  # every disagreement


def sample(records: list[dict[str, Any]], n: int, seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    tagged = [r for r in records if r.get("mpa_ids") or r.get("in_port")]
    first = rng.sample(tagged, min(len(tagged), n // 2))
    chosen = {id(r) for r in first}
    rest = [r for r in records if id(r) not in chosen]
    return first + rng.sample(rest, min(len(rest), n - len(first)))


def compare(records: list[dict[str, Any]], rows: list[tuple[Any, ...]], tolerance: float) -> Result:
    res = Result()
    for r, (_, eez, mpas, port, d_boundary, d_mpa) in zip(records, rows, strict=True):
        res.checked += 1
        res.eez_ids[str(r.get("eez_id"))] += 1
        res.in_mpa += bool(r.get("mpa_ids"))
        res.in_port += bool(r.get("in_port"))
        where = f"mmsi {r.get('mmsi')} at ({r.get('lon')}, {r.get('lat')})"
        if r.get("eez_id") != eez:
            res.eez += 1
            res.examples.append(f"{where}: eez_id {r.get('eez_id')}, PostGIS {eez}")
        if list(r.get("mpa_ids") or []) != list(mpas):
            res.mpa += 1
            res.examples.append(f"{where}: mpa_ids {r.get('mpa_ids')}, PostGIS {mpas}")
        if bool(r.get("in_port")) != port:
            res.port += 1
            res.examples.append(f"{where}: in_port {r.get('in_port')}, PostGIS {port}")
        expected_mpa = 0.0 if mpas else d_mpa
        for name, got, want in (("dist_eez_boundary_m", r.get("dist_eez_boundary_m"), d_boundary),
                                ("dist_mpa_m", r.get("dist_mpa_m"), expected_mpa)):
            if got is None or want is None:
                if got != want:
                    res.examples.append(f"{where}: {name} {got}, PostGIS {want}")
                continue
            diff = abs(float(got) - float(want))
            if name == "dist_eez_boundary_m":
                res.max_boundary_m = max(res.max_boundary_m, diff)
            else:
                res.max_mpa_m = max(res.max_mpa_m, diff)
            if diff > tolerance:
                res.examples.append(f"{where}: {name} {got}, PostGIS {want:.1f}")
    return res


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="zone_check", description=__doc__.splitlines()[0])
    ap.add_argument("run_id")
    ap.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "data" / "runs")
    ap.add_argument("--sample", type=int, default=400)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--tolerance", type=float, default=5.0, help="metres")
    args = ap.parse_args(argv)

    path = args.runs_dir / args.run_id / "produced.json"
    brokers = setting("REDPANDA_BROKERS", "localhost:19092")
    try:
        since = run_started_ms(path) - 60_000
        records = [r for r in read_topic(brokers, ENRICHED, since) if r.get("run_id") == args.run_id]
    except (RunError, FileNotFoundError) as e:
        print(f"zone_check: {e}", file=sys.stderr)
        return 2
    if not records:
        print(f"zone_check: no ais.enriched records for {args.run_id}", file=sys.stderr)
        return 2
    picked = sample(records, args.sample, args.seed)
    with psycopg.connect(conninfo()) as conn:
        rows = conn.execute(
            QUERY, {"lon": [float(r["lon"]) for r in picked], "lat": [float(r["lat"]) for r in picked]}
        ).fetchall()
    res = compare(picked, rows, args.tolerance)

    print(f"run {args.run_id}: {len(records)} enriched records, {res.checked} checked against PostGIS")
    print(f"  in a protected area {res.in_mpa}, in port {res.in_port}; eez_id: "
          + ", ".join(f"{k} {n}" for k, n in res.eez_ids.most_common()))
    print(f"  tags that differ: eez_id {res.eez}, mpa_ids {res.mpa}, in_port {res.port}")
    print(f"  largest difference: dist_eez_boundary_m {res.max_boundary_m:.2f} m, dist_mpa_m {res.max_mpa_m:.2f} m")
    for line in res.examples[:10]:
        print(f"  {line}")
    ok = not res.examples
    print("zone_check: OK" if ok else "zone_check: MISMATCH", file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
