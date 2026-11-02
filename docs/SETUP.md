# Dark Vessel Watch: Setup Guide

One-time setup before M0. Goal: after this, the whole pipeline runs with the network cable unplugged.

## 1. Accounts and registrations

| What | Where | Account needed? | When | Notes |
|---|---|---|---|---|
| Danish AIS daily CSVs | aisdata.ais.dk (DMA, now operated under the Danish Emergency Management Agency) | No | Now (M1) | Free download. Daily zips are large, multi-GB once unzipped. Pick one recent weekday and stick to it so numbers in the README are comparable. |
| EEZ boundaries | marineregions.org/downloads.php | Download request form (name, email, org, intended use) | Now (M3) | Licence is CC-BY-NC-SA. Fill "intended use" honestly: non-commercial portfolio/research. Download World EEZ (latest version) polygons **and** boundary lines (lines are what you need for "distance to boundary"). |
| Marine protected areas (WDPA) | protectedplanet.net | Click-through terms, no account | Now (M3) | Non-commercial only, **no redistribution**. Filter to MARINE = 1 or 2 at load time. |
| Natura 2000 | EEA data portal (eea.europa.eu) | No | Optional (M3) | Overlaps WDPA for Europe. Start with WDPA only; add Natura 2000 if Danish coverage looks thin. |
| World Port Index (Pub 150) | msi.nga.mil/Publications/WPI | No | Now (M3) | US Gov public domain. Download CSV or Shapefile. Ports are points: you will buffer them into port zones in PostGIS. |
| AISStream.io | aisstream.io | GitHub login, issues an API key | Later (optional `--source live`) | Keep the key in `.env`, never in the repo. Not needed for the offline demo. |
| GitHub repo | github.com | Yes (already have) | Now | Public repo is fine, but **no data files** (see licensing below). |

### Licensing consequence for the repo (important)

Marine Regions (NC-SA) and WDPA (no redistribution) mean the signed offline reference package **cannot be published** in the GitHub repo or as a release asset. The repo ships the *builder and verifier*; each user downloads the sources and builds their own package. This matches the CLAUDE.md rule "do not commit data files" and is worth one line in the README and an ADR.

Marine Regions and WDPA downloads sit behind a form / terms page, so they cannot be fully scripted. `scripts/fetch_refdata.sh` should automate WPI and DMA, and for the other two print instructions and wait for the zips to appear in `data/refdata/incoming/`.

## 2. Local installs

Host machine (laptop, ~8 GB RAM for the stack):

| Tool | Version | Why |
|---|---|---|
| Docker Engine + Compose v2 (Docker Desktop, OrbStack or Colima) | latest | Everything runs in Compose. Give the Docker VM **at least 10 GB RAM and 4 CPUs** so the 8 GB stack plus Flink later fits. |
| Python | 3.12 | Faust app, replayer, sinks, reconcile. Use `uv` to manage venv and lockfile. |
| uv | latest | Fast, reproducible `uv.lock`; also lets you build a wheelhouse for offline installs. |
| Java (Temurin) | 17 | Flink 1.20 job (M4). Install via SDKMAN or package manager. |
| Maven | 3.9.x | Flink job build. |
| rpk (Redpanda CLI) | match Redpanda image | Create topics, inspect offsets for reconciliation. Can also run via `docker exec`, so host install is optional. |
| GDAL (`ogr2ogr`) | 3.8+ | Shapefile/GeoPackage → PostGIS. Optional on host: you can run it from the `ghcr.io/osgeo/gdal` image instead, which keeps the toolchain in Docker. |
| minisign (or GPG) | latest | Signing the reference package (spec: "signature-verified before loading"). Minisign is simpler than GPG: one keypair, one `.minisig` per file. |
| sha256sum / shasum | built-in | Package checksums. |
| Claude Code | latest | Implementation. |
| Later (M7): k3d or kind, kubectl, Helm 3 | latest | Kubernetes deployment. Skip for now. |

Python dependencies (from CLAUDE.md): faust-streaming, rocksdict, numpy, polars, clickhouse-connect, psycopg[binary] 3, plus dev: mypy, ruff, pytest.

## 3. Docker images to pre-pull (offline readiness)

Pull these once while online, then `docker save` them to a tarball if you want to prove air-gapped operation on a clean machine.

| Image | Milestone | Memory budget |
|---|---|---|
| redpandadata/redpanda (pin a tag) | M0 | 1 GB (`--smp 1 --memory 1G --overprovisioned`) |
| redpandadata/console | M0 | 256 MB |
| clickhouse/clickhouse-server:24.8 (LTS) | M0 | 1.5 to 2 GB |
| postgis/postgis:16-3.4 | M0 | 512 MB |
| ghcr.io/osgeo/gdal (ubuntu-small) | M3 | run-once |
| flink:1.20-java17 | M4 | JM 1 GB + 1 TM 1.5 GB |
| prom/prometheus, grafana/grafana | M6 | ~512 MB total |

Pin every tag. `latest` breaks reproducibility, which is one of the things the project is judged on.

## 4. Signing key (one-time)

```
minisign -G -p refdata/keys/dvw.pub -s ~/.minisign/dvw.key
```

Commit the **public** key (`refdata/keys/dvw.pub`). The secret key never enters the repo. The verifier in `refdata/` checks the signature over a `manifest.json` (file list + SHA-256 + source version + download date) before anything is loaded into PostGIS.

## 5. Repo bootstrap

```
git init dark-vessel-watch && cd dark-vessel-watch
mkdir -p ingest/replayer flink/enrichment stream/faust_app sinks \
         sql/clickhouse sql/postgis refdata/keys reconcile docs/adr \
         deploy/compose deploy/helm scripts data
cp PROBLEM_STATEMENT.md CLAUDE.md docs/SETUP.md .
```

`.gitignore` must include `data/`, `.env`, `*.zip`, `*.csv`, `*.shp`, `*.gpkg`, `.venv/`, `target/`.

`.env.example` (committed):

```
REDPANDA_BROKERS=localhost:19092
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=dvw
CLICKHOUSE_PASSWORD=change-me
POSTGRES_DSN=postgresql://dvw:change-me@localhost:5432/dvw
AIS_DATA_DIR=./data/ais
REFDATA_DIR=./data/refdata
AISSTREAM_API_KEY=
```

## 6. Data layout (not committed)

```
data/
  ais/            aisdk-YYYY-MM-DD.zip + extracted CSV
  refdata/
    incoming/     raw downloads (EEZ, WDPA, WPI, Natura 2000)
    packages/     built, signed packages: dvw-refdata-<version>/
```

## 7. Checklist

- [ ] Marine Regions download form submitted, EEZ polygons + lines downloaded
- [ ] WDPA marine subset downloaded (shapefile or GeoPackage)
- [ ] WPI downloaded
- [ ] One DMA day downloaded and unzipped; row count noted
- [ ] Docker VM has ≥ 10 GB RAM, 4 CPUs
- [ ] Python 3.12 + uv, Java 17 + Maven installed
- [ ] minisign keypair generated, public key in repo
- [ ] Images pulled with pinned tags
- [ ] Repo bootstrapped, CLAUDE.md "Current milestone: M0"