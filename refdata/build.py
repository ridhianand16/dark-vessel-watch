"""Build the signed zone package from the pinned sources (ADR 0011).

    uv run python -m refdata build          # or: make refdata-build

Offline: reads only data/refdata/incoming/ (checked against refdata/sources.yaml),
refdata/area.yaml and refdata/package.yaml, and runs GDAL and minisign in the
refdata-tools container, which has no network.

Per layer, ogr2ogr first cuts the source to the package bbox at full detail (-spat
selects features, -clipsrc cuts their geometry, -makevalid repairs before cutting),
then a second pass renames columns to the loader's schema and drops anything the cut
left empty. WDPA keeps REALM Coastal or Marine (the 1.6 schema's name for the old
MARINE = 1 or 2). WPI points become circles of port_radius_nm (refdata/ports.py).

The package version is a hash of every input: source hashes, bbox, radius, filters,
tool image. The same inputs give the same version; GeoPackage timestamps are fixed
(OGR_CURRENT_DATE) so they also give the same bytes. A package that already exists
and verifies is reused.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from common.env import REPO_ROOT, setting
from refdata import gpkg, tools
from refdata.area import BBox, load_config
from refdata.fetch import Source, load_source, resolve_version, sha256_file, verify
from refdata.package import (
    FORMAT,
    LATEST,
    LAYERS,
    MANIFEST,
    PUBLIC_KEY,
    SIGNATURE,
    Layer,
    VerifyError,
    packages_dir,
    refdata_dir,
    trusted_comment,
    verify_package,
)
from refdata.ports import write_port_circles

SOURCES = REPO_ROOT / "refdata" / "sources.yaml"
AREA = REPO_ROOT / "refdata" / "area.yaml"
CONFIG = REPO_ROOT / "refdata" / "package.yaml"
WDPA_REALMS = ("Coastal", "Marine")
FIXED_DATE = "2000-01-01T00:00:00.000Z"  # gpkg_contents.last_change, so rebuilds are byte-identical


class BuildError(Exception):
    pass


@dataclass(frozen=True)
class Cut:
    """How one package layer is cut from its source."""

    layer: Layer
    source: str
    path: str
    """Path inside the source's incoming directory: a file name or a sources.yaml member."""
    src_layer: str
    fields: tuple[str, ...]
    where: str
    select: str
    """SQL over the cut layer 'clip', producing the loader's columns and geom."""


def cuts(versions: dict[str, str]) -> list[Cut]:
    wdpa = versions["wdpa_marine"]
    src = {k: f"{k}:{v}" for k, v in versions.items()}
    if any("'" in s for s in src.values()):
        raise BuildError(f"a source version contains a quote: {src}")
    keep = "geom IS NOT NULL AND NOT ST_IsEmpty(geom)"
    layer = {lay.name: lay for lay in LAYERS}
    realms = ", ".join(f"'{r}'" for r in WDPA_REALMS)
    return [
        Cut(
            layer=layer["eez"],
            source="eez",
            path="eez_v12.gpkg",
            src_layer="eez_v12",
            fields=("MRGID", "GEONAME"),
            where="",
            select="SELECT 'eez:' || MRGID AS zone_id, 'eez' AS kind, GEONAME AS name,"
            f" '{src['eez']}' AS source, geom FROM clip WHERE {keep} ORDER BY MRGID",
        ),
        Cut(
            layer=layer["mpa"],
            source="wdpa_marine",
            path=f"WDPA_WDOECM_{wdpa}_Public_marine.gdb",
            src_layer=f"WDPA_WDOECM_poly_{wdpa}_marine",
            fields=("SITE_PID", "NAME", "NAME_ENG", "DESIG_ENG", "REALM"),
            where=f"REALM IN ({realms})",
            # Natura 2000 sites often appear twice under one name (habitats and birds
            # directives), so the designation is part of the name.
            select="SELECT 'wdpa:' || SITE_PID AS zone_id, 'mpa' AS kind,"
            " COALESCE(NULLIF(NAME_ENG, ''), NAME) || COALESCE(' (' || NULLIF(DESIG_ENG, '') || ')', '') AS name,"
            f" '{src['wdpa_marine']}' AS source, geom FROM clip WHERE {keep} ORDER BY SITE_PID",
        ),
        Cut(
            layer=layer["ports"],
            source="wpi",
            path="work/port_circles.geojson",
            src_layer="port_circles",
            fields=("INDEX_NO", "PORT_NAME", "COUNTRY"),
            where="",
            select="SELECT 'wpi:' || INDEX_NO AS zone_id, 'port' AS kind, PORT_NAME || ', ' || COUNTRY AS name,"
            f" '{src['wpi']}' AS source, geom FROM clip WHERE {keep} ORDER BY INDEX_NO",
        ),
        Cut(
            layer=layer["eez_boundaries"],
            source="eez",
            path="eez_boundaries_v12.gpkg",
            src_layer="eez_boundaries_v12",
            fields=("LINE_ID",),
            where="",
            select=f"SELECT LINE_ID AS id, geom FROM clip WHERE {keep} ORDER BY LINE_ID",
        ),
    ]


def find(root: Path, name: str) -> Path:
    found = sorted(root.rglob(name))
    if not found:
        raise BuildError(f"{name} not found under {root}; run make fetch-refdata")
    return found[0]


def read_area(path: Path) -> dict[str, Any]:
    doc: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or "bbox" not in doc:
        raise BuildError(f"{path} has no bbox; run make area")
    return doc


def version_of(inputs: dict[str, Any]) -> str:
    canonical = json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()
    return "zones-" + hashlib.sha256(canonical).hexdigest()[:12]


def ogr_cut(cut: Cut, src: str, bbox: BBox, build_dir: Path) -> None:
    """Cut src into build_dir/<layer>.gpkg. src is a path inside the container (/in or /build)."""
    box = [str(v) for v in (bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat)]
    mounts = [tools.Mount(refdata_dir() / "incoming", "/in"), tools.Mount(build_dir, "/build", read_only=False)]
    env = {"OGR_CURRENT_DATE": FIXED_DATE}
    clip = f"/build/work/{cut.layer.name}.clip.gpkg"
    common = ["-f", "GPKG", "-lco", "GEOMETRY_NAME=geom", "-lco", "SPATIAL_INDEX=NO", "-nlt", cut.layer.geometry]
    first = ["ogr2ogr", *common, "-nln", "clip", "-select", ",".join(cut.fields)]
    if cut.where:
        first += ["-where", cut.where]
    first += ["-spat", *box, "-clipsrc", "spat_extent", "-makevalid", clip, src, cut.src_layer]
    tools.run(first, mounts, env)
    out = f"/build/{cut.layer.name}.gpkg"
    tools.run(["ogr2ogr", *common, "-nln", cut.layer.name, "-sql", cut.select, out, clip], mounts, env)


def build(key_path: Path, public_key: Path = PUBLIC_KEY) -> Path:
    cfg = load_config(CONFIG)
    area = read_area(AREA)
    b = area["bbox"]
    bbox = BBox(float(b["min_lon"]), float(b["min_lat"]), float(b["max_lon"]), float(b["max_lat"]))

    incoming = refdata_dir() / "incoming"
    sources: dict[str, Source] = {k: load_source(SOURCES, k) for k in ("eez", "wdpa_marine", "wpi")}
    versions: dict[str, str] = {}
    for key, source in sources.items():
        versions[key] = resolve_version(source)
        verify(source, incoming / key / versions[key] / source.file)  # raises FetchError on mismatch

    plan = cuts(versions)
    inputs: dict[str, Any] = {
        "format": FORMAT,
        "tools_image": f"{tools.IMAGE} ({tools.GDAL_IMAGE})",
        "area": {"ais_day": area["ais_day"], "bbox": bbox.as_dict(), "margin_nm": float(area["margin_nm"])},
        "port_radius_nm": cfg.port_radius_nm,
        "wdpa_realms": list(WDPA_REALMS),
        "sources": {
            k: {
                "name": s.name,
                "version": versions[k],
                "file": s.file,
                "sha256": s.expected_sha256,
                "licence": s.licence,
                "attribution": s.attribution,
            }
            for k, s in sources.items()
        },
        "layers": {
            c.layer.name: {"source": c.source, "from": c.src_layer, "where": c.where, "select": c.select} for c in plan
        },
    }
    version = version_of(inputs)
    final = packages_dir() / version
    if final.exists():
        try:
            verify_package(final, public_key)
        except VerifyError as exc:
            raise BuildError(f"{final} exists but does not verify ({exc}). Delete it to rebuild.") from exc
        print(f"build: {version} already built and verified, reusing it", file=sys.stderr)
        _write_latest(version)
        return final
    if not key_path.exists():
        raise BuildError(f"signing key {key_path} not found; run make refdata-key once")

    tmp = packages_dir() / f".{version}.tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    (tmp / "work").mkdir(parents=True)
    try:
        wpi_dir = incoming / "wpi" / versions["wpi"]
        circles = tmp / "work" / "port_circles.geojson"
        n_ports = write_port_circles(wpi_dir / sources["wpi"].file, circles, bbox, cfg.port_radius_nm)
        print(f"build: {n_ports} WPI ports near the area, {cfg.port_radius_nm:g} nm circles", file=sys.stderr)

        files: dict[str, Any] = {}
        for cut in plan:
            if cut.path.startswith("work/"):
                src = f"/build/{cut.path}"
            else:
                rel = find(incoming / cut.source / versions[cut.source], cut.path).relative_to(incoming)
                src = f"/in/{rel.as_posix()}"
            print(f"build: cutting {cut.layer.name} from {cut.src_layer}", file=sys.stderr)
            ogr_cut(cut, src, bbox, tmp)
            out = tmp / f"{cut.layer.name}.gpkg"
            _, geometry_type, srs_id = gpkg.geometry_column(out, cut.layer.name)
            if srs_id != 4326:
                raise BuildError(f"{out.name}: SRS {srs_id}, expected 4326")
            files[out.name] = {
                "layer": cut.layer.name,
                "table": cut.layer.table,
                "geometry": geometry_type,
                "features": gpkg.count(out, cut.layer.name),
                "bytes": out.stat().st_size,
                "sha256": sha256_file(out),
            }
            print(f"build: {cut.layer.name}: {files[out.name]['features']} features", file=sys.stderr)
        shutil.rmtree(tmp / "work")

        manifest = {"package_version": version, **inputs, "files": files}
        (tmp / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tools.run(
            ["minisign", "-S", "-s", "/key/dvw.key", "-m", f"/build/{MANIFEST}", "-t", trusted_comment(version)],
            [tools.Mount(key_path, "/key/dvw.key"), tools.Mount(tmp, "/build", read_only=False)],
        )
        tmp.rename(final)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    verify_package(final, public_key)  # catches a signing key that does not match refdata/keys/dvw.pub
    _write_latest(version)
    print(f"build: wrote and verified {final} ({SIGNATURE} over {MANIFEST})", file=sys.stderr)
    return final


def _write_latest(version: str) -> None:
    (packages_dir() / LATEST).write_text(version + "\n", encoding="utf-8")


def signing_key() -> Path:
    return Path(setting("MINISIGN_KEY", str(Path.home() / ".minisign" / "dvw.key"))).expanduser()
