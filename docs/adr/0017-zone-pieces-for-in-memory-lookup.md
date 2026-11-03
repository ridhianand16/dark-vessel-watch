# 0017. Zone pieces of at most 256 vertices for in-memory lookup

Status: accepted (2026-11-04)

## Context

The Flink enrichment job tags every AIS record with the EEZ, protected areas and port
it is in, and the distance to the nearest EEZ boundary and protected area. It holds the
active zone package in memory and queries it with JTS (ADR 0013).

The package is not small. `zones_25832` has 3,026,879 vertices: 2,928,330 in 2,169
protected areas, 87,213 in 8 EEZs, 11,336 in 176 port circles. One Natura 2000 site
alone has 977,787. An STRtree over whole zones only narrows a lookup down to the zones
whose bounding box holds the point. After that, plain point-in-polygon is a ray-crossing
scan of every vertex. Near the Danish coast, many points fall inside that one site's
box, so a single record could cost a million vertex comparisons.

JTS can index a polygon's edges for point-in-polygon (`IndexedPointInAreaLocator`), but
that index is about one tree node per segment: roughly 170 MB for this package on top of
the coordinates. The TaskManager has about 410 MB of task heap for everything.

## Decision

The loader cuts every metric zone and boundary into pieces of at most 256 vertices with
`ST_Subdivide`, into two new tables (migration 0004):

- `zone_pieces_25832 (zone_id, piece, kind, package_version, geom Polygon)`
- `eez_boundary_pieces_25832 (id, piece, package_version, geom LineString)`

`ST_Subdivide` cuts along straight lines in the layer's own coordinates, so the pieces
of a zone are exactly the zone, split up: no overlap, no gap. Multi-part results are
split with `ST_Dump`, so every piece is one polygon or one line.

This runs in the staging transaction, after the metric copies pass their checks, and
the pieces swap in with the other tables. Checks on the pieces:

- the usual staging checks: rows present, SRID 25832, valid;
- coverage: every source row has at least one piece, and its pieces add up to its area
  (polygons) or length (lines) within a relative 1e-9.

The pieces skip the bbox extent check. A cut adds vertices on segments that are
straight in UTM, and along the bbox's northern edge such a segment runs slightly
north of the parallel. On the real package the furthest cut vertex is 22 cm north of
the bbox, which the 1 cm tolerance for reprojected layers rejects. Coverage is the stronger check: pieces that add up to a
source row that passed the extent check cannot reach further than it does.

With the real package: 34,535 zone pieces (3,109,058 vertices, 2.7% more than the
source, from the cuts) and 171 boundary pieces. A lookup is an STRtree query on the
piece boxes, then a scan of at most 256 vertices per candidate.

## Alternatives

- **Cut at read time** (`ST_Subdivide` in the Flink job's query). It keeps the loader
  unchanged, but the query takes 18 s on every job start, against 0.15 s to read the
  stored pieces. AIS records that arrive before the zones are buffered, so
  read time is buffer size. It also re-cuts identical geometry on every start.
- **Index whole polygons in JTS** (`IndexedPointInAreaLocator`, `IndexedFacetDistance`).
  These give exact results with no preparation, at about 170 MB of extra heap.
- **Simplify the protected areas.** At a 10 m tolerance most of the coastline detail
  goes and memory drops several times. But containment and distances would then
  disagree with PostGIS by up to the tolerance, near exactly the coastlines where
  vessels are. Simplifying should be a deliberate, versioned property of the package
  if it is ever needed.
- **A different piece size.** Fewer vertices per piece means more pieces, a deeper
  tree and more cut vertices. 256 is the `ST_Subdivide` default, and lookups are
  already well under the per-record budget. It is one constant in `refdata/load.py`.

## Consequences

- A load takes about 39 s instead of about 13 s; the extra time is `ST_Subdivide` on the
  protected areas. Readers are still only blocked for the swap (47 ms).
- PostGIS holds a third metric copy of the layers (48 MB of WKB for the zone pieces).
- Cut lines are not real boundaries. Distances to a protected area must be taken to the
  nearest piece only when the point is outside every piece, because then the nearest
  point is on the real boundary. Inside a piece the distance is 0. The Flink job
  relies on this.
- Consumers that need whole zones (alerts, the console, investigators) keep using
  `zones` and `zones_25832`.
