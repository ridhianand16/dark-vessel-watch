# 0013. Metric copies of the zone layers in EPSG:25832, and an explicit active package

Status: accepted (2026-11-03)

## Context

The Flink enrichment job will tag every AIS message with its EEZ, protected areas and
port, and with distances in metres to the nearest EEZ boundary and protected area.
It reads the active package from PostGIS once at start, keeps the zones in memory, and
queries them with JTS. JTS is planar: given EPSG:4326 coordinates it measures in
degrees, and a degree of longitude in the package area is between 56 and 66 km
depending on latitude. So the job needs the zones in a metric projection. It should
also get them already validated, so that every Flink task does not repeat the
cleaning that the loader's staging checks already do (ADR 0010).

The job also needs one answer to "which package is live". Until now the active package
was implied: the latest `loaded_at` in `refdata_packages`. A startup gate that waits
for zones and an alert that records the zone version both need that answer, and an
implied rule is easy to get subtly wrong (ties, a failed load, a rollback that reloads
an older version).

## Decision

**A metric copy of each layer, written by the loader.** Migration 0003 adds
`zones_25832` and `eez_boundaries_25832`. They have the same columns as `zones` and
`eez_boundaries`, with geometry in EPSG:25832 (ETRS89 / UTM zone 32N, metres) and the
same GiST indexes. In the staging transaction, after the 4326 layers pass the staging
checks (which repair invalid geometries with `ST_MakeValid`), the loader:

1. densifies each repaired geometry, then `ST_Transform`s it to 25832;
2. runs the same staging checks on the copies: rows present, SRID 25832, valid (repaired
   and logged if reprojection broke one), extent inside the package bbox (compared in
   degrees after transforming back, with a 1 cm tolerance for the round trip);
3. checks the copies have exactly as many rows as their 4326 sources;
4. swaps all four tables in with the same renames as before.

A reader never sees 4326 and 25832 tables from different packages.

**Densify before transforming, differently per layer.** A segment in the source data is
a straight line between two vertices, and EEZ boundary segments run to about 50 km.
`ST_Transform` moves only the vertices. Unless the segment is densified first, it
becomes a straight line in UTM, which is a different path on the ground. Undensified,
the Kattegat check point measured 9,040.3 m to the nearest boundary in 25832 against
9,046.6 m with geography. 0.7 m of that is projection scale and 5.6 m is segment shape.

- Boundary lines are densified along the geodesic (`ST_Segmentize(geom::geography,
  5000)`). Distances are what they are used for, and with this the metric distance
  agrees with the geography distance up to the projection's scale error.
- Zone polygons are densified in degrees (`ST_Segmentize(geom, 0.05)`). That keeps the
  edges straight in lon/lat, which is how the GeoPackages and `ST_Contains` on `zones`
  read them, so Flink's point-in-polygon results agree with SQL on the 4326 table. A
  geodesic would also bow the clip edge along the bbox's northern parallel about 700 m
  north of the bbox, and the staging extent check rejects that.

At 5 km and 0.05 degrees, densifying more changes the Kattegat result by less than a
centimetre. It adds 7,700 vertices to the 3.02 million in `zones` (0.25%) and 1,100 to
the 13,339 boundary vertices.

**25832 finds the nearest point; the reported metres are geodesic.** The Flink job uses
the 25832 STRtree only to find the nearest point on a boundary or protected area. It
then transforms that point back to 4326 and reports the geodesic distance from the
vessel to it (GeographicLib, WGS84 ellipsoid). A point found in UTM is slightly off the
true nearest point, but the distance along a line is flat at its minimum, so that error
is second order. On the real package this matches PostGIS geography to 0.1 m at
11.6 E, 15.5 E and 17.3 E, where UTM alone was 0.7 m, 31 m and 9 m off. Point-in-polygon
(EEZ, protected area, port) stays planar in 25832: containment does not depend on scale.

**An explicit active marker.** Migration 0003 adds `refdata_packages.active`, with a
partial unique index that allows at most one active row. Inside the swap transaction
the loader clears the old marker and inserts the new load with `active = true`. The
active version is:

```sql
SELECT package_version FROM refdata_packages WHERE active;
```

Rows from loads before migration 0003 stay inactive, because they wrote no metric
copies. Until the next `refdata load`, the query returns nothing and the Flink startup
gate stays closed.

## Alternatives

- **Reproject in Flink.** Every task would transform about 3 million vertices on each
  start, on the JVM, with GeoTools or proj4j instead of the PROJ that PostGIS uses. The
  copies would not be checked and could differ between libraries. Reference-data
  preparation belongs to the reference-data stage, where it is done once, checked, and
  versioned with the package.
- **Geodesic math in Flink on 4326** (GeographicLib distances, spherical point in
  polygon). This is exact everywhere, but JTS indexes and predicates are planar. It
  would mean writing nearest-segment search and containment ourselves.
- **A view or generated column instead of a table.** `ST_Transform` would run on every
  read, and a view gets no GiST index of its own. A stored generated column could not
  densify differently per layer without a function, and it would make the 4326 tables
  wider for every reader.
- **A projection centred on the area**, a custom transverse Mercator at about 10.1 E,
  roughly the middle of the bbox. It halves the worst-case scale error at the bbox
  edges, but it has no EPSG code. Investigators and other tools would need the proj
  string. EPSG:25832 is the Danish national grid and every GIS tool knows it.
- **Keep "latest loaded_at" as the active rule.** That works today with one loader and
  no rollback, but every reader has to repeat the rule. A marker set in the swap
  transaction says the same thing in one place, and the database enforces it.

## Consequences

- PostGIS holds two copies of each layer, plus a third during a load. `zones_25832` is
  49 MB with its indexes, and a load takes about 13 s instead of about 7 to 11 s.
- UTM 32N is exact at 9 E and stretches with distance from it. Scale errors measured on
  the package: -0.008% at 11.6 E (Kattegat; 0.7 m on a 9 km distance) and +0.16% at
  15.5 E (Bornholm; 31 m on 19.3 km), about +0.3% at the bbox's eastern edge (17.5 E).
  Because reported distances are geodesic, none of this reaches `dist_eez_boundary_m`
  or `dist_mpa_m` or the DARK_GAP thresholds. Planar distances in 25832 are only for
  ranking candidates. Anyone measuring directly in the 25832 tables in SQL gets the
  scale error.
- Each distance costs one inverse transform and one geodesic calculation per message,
  on top of the tree lookup.
- PostGIS geography is the reference for the Flink job's distance tests. A loader test
  checks the method near Bornholm in SQL; the job's own test has to match it in Java.
- The metric copies' `package_version` comes from the same rows as the 4326 tables, so
  Flink can stamp `zones_version` from either.
- Rollback still means reloading an older package. It becomes the active one, because
  the marker follows the latest swap, not the highest version.
