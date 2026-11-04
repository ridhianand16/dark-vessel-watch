# Data

The pipeline runs on data I do not own, and none of it is in this repository. This page covers where each dataset comes from, its licence, which files have to be downloaded by hand, and how the zone package is built from them. Commands are in [operations.md](operations.md).

## Sources

Every source is pinned by version and SHA-256 in `refdata/sources.yaml`. A file whose hash does not match is refused: upstream changed under the same version.

| Data | Source | Version | How it is fetched | Used for |
|---|---|---|---|---|
| AIS positions | Danish Maritime Authority daily CSV, `aisdk-{day}.zip` on their S3 bucket | 2026-10-28 (3.2 GB CSV, 18.9 million rows) | `make fetch` downloads it | the replay |
| EEZ polygons and boundary lines | Marine Regions World EEZ, GeoPackage | v12 | by hand (behind a form) | DARK_GAP's distance to an EEZ boundary |
| Marine protected areas | Protected Planet WDPA monthly release, realm Coastal or Marine | November 2026 (`WDPA_VERSION`) | by hand (terms page) | DARK_GAP's distance to a protected area |
| Ports | World Port Index (Pub 150), NASA Harvest mirror of the NGA release | 2019 | by hand (NGA refuses scripted downloads) | DARK_GAP's "not in port" |

`AIS_DAY` in `.env` must match `dma_ais.version` in `refdata/sources.yaml`, and `WDPA_VERSION` must match `wdpa_marine.version`.

## Manual downloads

All three zone sources are marked `manual: true` in `refdata/sources.yaml`, so the fetcher never downloads them: EEZ sits behind a form, WDPA moves monthly, and NGA refuses scripted WPI downloads. Download each one into `data/refdata/incoming/<source>/<version>/`, under the file name in `refdata/sources.yaml`. `make fetch-refdata` verifies what is there, prints the download page and the exact target path for anything missing, and fails on any file whose SHA-256 does not match ([ADR 0009](adr/0009-zone-sources-pinned-and-area-from-the-feed.md)).

The EEZ boundary lines ship in the same zip as the polygons. The WFS service is not used: its bytes change with the server and cannot be pinned.

Local data layout (never committed):

```
data/
  ais/            aisdk-YYYY-MM-DD.zip and the extracted CSV
  runs/<run_id>/  produced.json (and labels.json) per replay: the input to reconciliation
  refdata/
    incoming/     downloads per source and version
    packages/     built, signed zone packages
```

## Licences and attribution

- **AIS positions:** Contains AIS data from the Danish Maritime Authority. Free re-use under the Danish act on re-use of public sector information.
- **EEZ polygons and boundaries:** Flanders Marine Institute (2023). Maritime Boundaries Geodatabase: Maritime Boundaries and Exclusive Economic Zones (200NM), version 12. Available online at https://www.marineregions.org/. CC BY-NC-SA 4.0.
- **Marine protected areas:** UNEP-WCMC and IUCN (2026), Protected Planet: The World Database on Protected Areas (WDPA), November 2026, Cambridge, UK: UNEP-WCMC and IUCN. Available at: www.protectedplanet.net. Non-commercial use; no redistribution.
- **Ports:** National Geospatial-Intelligence Agency, World Port Index (Pub 150); obtained via NASA Harvest Portal. US Government work, public domain (original); CC BY on the mirror.

Because of the WDPA and Marine Regions terms, the built zone package is never committed or published ([ADR 0011](adr/0011-signed-zone-package-built-from-pinned-sources.md)). The repository ships the fetcher, the pinned hashes, the package builder and the public key instead. Anyone else builds and signs their own package from their own downloads.

## The area

The zone package covers only the area the feed sees. `make area` computes it from the AIS day and writes `refdata/area.yaml`: 2.69 to 17.53 E, 53.52 to 59.57 N, which is the trimmed extent of 18.9 million valid positions plus a 20 nm margin (twice the largest "within N nm" rule, set in `refdata/package.yaml`). Plain min/max would have reached 60 S, because of 179 stray fixes.

## Zone package

DARK_GAP needs to know whether a vessel went quiet near an EEZ boundary or a protected area, and not in port. Those shapes come from the three zone sources, cut to the area at full detail:

| Layer | From | Rows | Vertices |
|---|---|---|---|
| EEZ polygons (`zones`, kind `eez`) | Marine Regions World EEZ v12 | 8 | 85,526 |
| EEZ boundary lines (`eez_boundaries`) | Marine Regions World EEZ v12 | 103 | 13,339 |
| Protected areas (`zones`, kind `mpa`) | WDPA November 2026, realm Coastal or Marine | 2,169 | 2,922,333 |
| Ports (`zones`, kind `port`) | World Port Index 2019, each point a 2 nm circle | 176 | 11,327 |

Vertices are `SUM(ST_NPoints(geom))` per layer after loading. Protected areas account for 96% of them, because Natura 2000 sites follow the coastline closely.

How a package is made and loaded:

1. **Build** (`make refdata-build`). The build re-checks the SHA-256 of every source in `refdata/sources.yaml` and extracts the zip sources again into its own work directory, so it reads only the bytes those hashes name, never a file an earlier unzip left in `incoming/`. `ogr2ogr` then cuts each layer to `refdata/area.yaml`, in a container with no network. Port circles are computed on the WGS84 ellipsoid, and each vertex is within 0.5 m of 2 nm as PostGIS geography measures it ([ADR 0010](adr/0010-zone-package-staging-swap-and-port-radius.md)). The output is one GeoPackage per layer and a `manifest.json` with every input and file hash, signed with minisign. The package version is a hash of the inputs, and two builds from the same inputs are byte-identical ([ADR 0011](adr/0011-signed-zone-package-built-from-pinned-sources.md)).
2. **Verify** (`make refdata-verify`). The signature over the manifest is checked against the committed `refdata/keys/dvw.pub`, then every file's hash. Changing one byte in any file fails this step.
3. **Load** (`make refdata-load`). The loader verifies again, then fills a staging schema, checks row counts and geometry validity there, writes the metric copies and pieces described below, and swaps all the tables into place. All of that is one transaction, so a package that fails its signature, a file hash or a staging check is refused as a whole and the live zones stay as they were. Each load adds a row to `refdata_packages` and marks it as the one active package. Loading the same package twice gives identical zones and two registry rows.

### Metric copies

The Flink job keeps the zones in memory and measures with JTS, which is planar. So the loader also writes `zones_25832` and `eez_boundaries_25832`: the same rows in EPSG:25832 (UTM zone 32N, the Danish metric grid), already repaired, checked and GiST-indexed ([ADR 0013](adr/0013-metric-zone-copy-and-active-package.md)). Before reprojecting, it adds vertices to long segments: geodesic for boundary lines, so a metric distance matches a geography distance, and straight in degrees for polygons, so containment matches `zones`. Without that, a 50 km boundary segment becomes a different line in UTM and a Kattegat distance was 6 m off. With it, what remains is the projection's scale error: 0.7 m on 9 km in the Kattegat, but 0.16% around Bornholm and about 0.3% at the eastern edge of the area. So the job uses 25832 only to find the nearest point, then reports the geodesic distance to it. Near Bornholm that gives 19,324.5 m, the same as PostGIS geography, where UTM alone says 19,355.2 m.

### Pieces

One protected area alone has 977,787 vertices, and a point-in-polygon test scans all of them. So the loader also cuts every metric zone and boundary into pieces of at most 256 vertices (`ST_Subdivide`) and checks that each zone's pieces add up to its area and each boundary's to its length. That gives 34,535 zone pieces and 171 boundary pieces. A lookup in the job is then a tree query plus a scan of a few hundred vertices ([ADR 0017](adr/0017-zone-pieces-for-in-memory-lookup.md)).

### Querying the zones

```sql
SELECT package_version FROM refdata_packages WHERE active;   -- the live package
```

To find the nearest boundary in metres, order by geography distance. `ORDER BY geom <-> point` on geometry ranks in degrees, and at 57 N it puts a 37.6 km line ahead of a 30.2 km one:

```sql
SELECT id, round(ST_Distance(geom::geography, p::geography)) AS metres
FROM eez_boundaries, ST_SetSRID(ST_Point(11.3, 56.9), 4326) AS p   -- mid-Kattegat
ORDER BY 2 LIMIT 1;                                                -- line 1120, 30,241 m
```

### Signing key

`make refdata-key` creates the signing key: the secret key in `MINISIGN_KEY` (default `~/.minisign/dvw.key`, never in the repository) and the public key in `refdata/keys/dvw.pub`, which is committed. The key has no password, so builds run unattended. `refdata/keys/dvw.pub` in the repository belongs to my key, so on another machine delete it before `make refdata-key` and use your own.
