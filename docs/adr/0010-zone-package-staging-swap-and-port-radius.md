# 0010. Swap zone packages in from a checked staging schema; ports get one fixed radius

Status: accepted (2026-11-03)

## Context

The zone package (ADR 0009) is loaded into PostGIS and read by the DARK_GAP rule,
by the map, by investigators in SQL, and later by the Flink zone-tagging job. A new
package version arrives when WDPA publishes a month or a source is re-pinned. The
loader has to replace the live zones without readers ever seeing a half-loaded
package or a broken geometry. It also has to be possible to say which package
version an alert was raised under.

Separately, ports need a shape. WPI gives one point per port with a harbour size
class, not a port boundary.

## Decision

**Replacing a package version.** The loader:

1. verifies the package (minisign signature over `manifest.json`, then each file's
   SHA-256 from the manifest);
2. loads every layer into a staging schema;
3. runs the staging checks (`refdata/staging.py`) inside the swap transaction:
   - each layer has rows;
   - no row lacks a geometry;
   - every geometry is in EPSG:4326;
   - every geometry passes `ST_IsValid`; invalid ones are repaired with
     `ST_MakeValid` and the count is logged, and anything still invalid or empty
     fails;
   - each layer's extent lies inside the bbox in `refdata/area.yaml`.

   Every problem is collected, and any problem aborts;
4. swaps staging into live by renaming, in the same transaction.

Readers wait only for the rename, not for the load. A failed check rolls back the
repairs and the swap together, and the live zones stay as they were.
`refdata_packages` records every load: version, manifest hash, verified_at and
loaded_at. Rollback means reloading the previous package from
`data/refdata/packages/`. Alerts record the active package version.

**Port zones.** Each WPI point is buffered by one radius, `port_radius_nm` in
`refdata/package.yaml` (default 2 nm). The buffer is computed as geography, so
the radius is metres on the ground at Danish latitudes, not degrees. The radius
is recorded in the package manifest.

## Alternatives

- **Load in place (truncate and insert in one transaction).** No second schema,
  but readers are locked out for the whole load. That includes clipping and
  indexing under emulation on an Apple Silicon laptop, and later Flink's
  refresh would stall for it. Validity problems would also only show up in the
  live tables.
- **One schema per package version, with views pointing at the active one.**
  This makes rollback a view switch. But every reader goes through views, old
  versions pile up and need a clean-up policy, and the extra flexibility is not
  needed while rollback by reloading takes seconds on a package this size.
- **Repair geometries at build time only.** The builder should produce valid
  shapes, but GEOS versions differ between the build machine and the PostGIS
  image (the compose image has GEOS 3.9). A shape valid on one can be invalid on
  the other, so the check belongs where the data is read.
- **Port radius from WPI harbour size.** Closer to reality for very large or
  very small harbours, but it adds a lookup table to tune. Most port cases never
  reach the zone check: a vessel arriving in port slows below DARK_GAP's 2 kn
  before it stops reporting. A port zone only matters for a vessel that goes dark
  while still moving near a port, and a fixed 2 nm covers the approach.
- **Port polygons from OpenStreetMap.** Better shapes, but another licence
  (ODbL), another source to pin, and uneven coverage.

## Consequences

- Each load briefly needs two copies of the zones in PostGIS. That is small for a
  Danish-area package.
- A package that would load a broken layer is refused as a whole, with a message
  listing every reason. The repaired-geometry count is in the loader's log.
- Port false positives (a gap alert just outside a large harbour, or inside a
  small one) are possible. The injected-anomaly scores are the signal to revisit
  this with size-based radii.
- The staging checks exist and are tested against the compose PostGIS. The
  package builder, verifier, loader, `refdata_packages` table and manifest are
  still to be built. `port_radius_nm` is the value they will buffer with and
  record.
