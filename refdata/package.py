"""The zone package on disk, and how it is verified (ADR 0011).

A package is a directory data/refdata/packages/<package_version>/ holding one
GeoPackage per layer, manifest.json and manifest.json.minisig. The signature covers the
manifest; the manifest lists every file with its size and SHA-256. Verifying:

    1. the minisign signature over manifest.json, against refdata/keys/dvw.pub, and the
       trusted comment naming this package version;
    2. the manifest: format, and package_version equal to the directory name;
    3. every listed file's size and SHA-256. A file in the directory that the manifest
       does not list also fails.

Nothing is parsed from the manifest before its signature has been checked.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.env import REPO_ROOT, setting
from refdata.area import BBox
from refdata.fetch import sha256_file
from refdata.minisign import SignatureError, read_public_key, read_signature, verify

FORMAT = 1
MANIFEST = "manifest.json"
SIGNATURE = MANIFEST + ".minisig"
LATEST = "LATEST"  # in the packages directory: the version the last build produced
PUBLIC_KEY = REPO_ROOT / "refdata" / "keys" / "dvw.pub"


def trusted_comment(version: str) -> str:
    return f"dvw zone package {version}"


def refdata_dir() -> Path:
    path = Path(setting("REFDATA_DIR", "./data/refdata"))
    return path if path.is_absolute() else REPO_ROOT / path


def packages_dir() -> Path:
    return refdata_dir() / "packages"


@dataclass(frozen=True)
class Layer:
    name: str
    """File stem and GeoPackage layer name."""
    table: str
    """Live PostGIS table it is loaded into."""
    geometry: str
    """Geometry type, as ogr2ogr -nlt and PostGIS spell it."""
    columns: tuple[str, ...]
    """Attribute columns the loader reads, in table column order."""


ZONE_COLUMNS = ("zone_id", "kind", "name", "source")
LAYERS = (
    Layer("eez", "zones", "MULTIPOLYGON", ZONE_COLUMNS),
    Layer("mpa", "zones", "MULTIPOLYGON", ZONE_COLUMNS),
    Layer("ports", "zones", "MULTIPOLYGON", ZONE_COLUMNS),
    Layer("eez_boundaries", "eez_boundaries", "MULTILINESTRING", ("id",)),
)
TABLES = ("zones", "eez_boundaries")


class VerifyError(Exception):
    pass


@dataclass(frozen=True)
class FileEntry:
    name: str
    layer: Layer
    features: int
    size: int
    sha256: str


@dataclass(frozen=True)
class VerifiedPackage:
    path: Path
    version: str
    manifest: dict[str, Any]
    manifest_sha256: str
    verified_at: dt.datetime
    files: tuple[FileEntry, ...]

    @property
    def bbox(self) -> BBox:
        b = self.manifest["area"]["bbox"]
        return BBox(float(b["min_lon"]), float(b["min_lat"]), float(b["max_lon"]), float(b["max_lat"]))

    def features(self, table: str) -> int:
        return sum(f.features for f in self.files if f.layer.table == table)


def _file_entries(manifest: dict[str, Any]) -> tuple[FileEntry, ...]:
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise VerifyError("manifest has no 'files' mapping")
    by_name = {f"{layer.name}.gpkg": layer for layer in LAYERS}
    if set(files) != set(by_name):
        raise VerifyError(f"manifest lists {sorted(files)}, expected {sorted(by_name)}")
    entries = []
    for name, layer in by_name.items():
        e = files[name]
        try:
            entries.append(FileEntry(name, layer, int(e["features"]), int(e["bytes"]), str(e["sha256"])))
        except (KeyError, TypeError, ValueError) as exc:
            raise VerifyError(f"manifest entry for {name} is malformed: {exc}") from exc
    return tuple(entries)


def verify_package(path: Path, public_key: Path = PUBLIC_KEY) -> VerifiedPackage:
    """Check signature, manifest and every file. Raises VerifyError naming the first problem."""
    try:
        key = read_public_key(public_key.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise VerifyError(f"public key {public_key} not found (make refdata-key creates it)") from exc
    try:
        manifest_bytes = (path / MANIFEST).read_bytes()
        signature = read_signature((path / SIGNATURE).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise VerifyError(f"{path}: {exc.filename} is missing") from exc
    except SignatureError as exc:
        raise VerifyError(f"{path / SIGNATURE}: {exc}") from exc

    try:
        comment = verify(manifest_bytes, signature, key)
    except SignatureError as exc:
        raise VerifyError(f"{path / MANIFEST}: signature check failed: {exc}") from exc

    manifest: Any = json.loads(manifest_bytes)
    if not isinstance(manifest, dict) or manifest.get("format") != FORMAT:
        raise VerifyError(f"{MANIFEST}: not a format {FORMAT} zone package manifest")
    version = str(manifest.get("package_version", ""))
    if comment != trusted_comment(version):
        raise VerifyError(f"signature's trusted comment {comment!r} does not name package {version!r}")
    if path.name != version:
        raise VerifyError(f"directory {path.name} holds package {version}")

    files = _file_entries(manifest)
    expected = {f.name for f in files} | {MANIFEST, SIGNATURE}
    extra = sorted(p.name for p in path.iterdir() if p.name not in expected and not p.name.startswith("."))
    if extra:
        raise VerifyError(f"{path}: files not in the manifest: {', '.join(extra)}")
    for f in files:
        file_path = path / f.name
        if not file_path.is_file():
            raise VerifyError(f"{file_path} is missing")
        size = file_path.stat().st_size
        if size != f.size:
            raise VerifyError(f"{f.name}: {size} bytes, manifest says {f.size}")
        actual = sha256_file(file_path)
        if actual != f.sha256:
            raise VerifyError(f"{f.name}: SHA-256 {actual} does not match the manifest's {f.sha256}")

    return VerifiedPackage(
        path=path,
        version=version,
        manifest=manifest,
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        verified_at=dt.datetime.now(dt.UTC),
        files=files,
    )


def resolve(package: Path | None) -> Path:
    """The package to verify or load: the given directory, or the one the last build named."""
    if package is not None:
        return package
    latest = packages_dir() / LATEST
    if not latest.exists():
        raise VerifyError(f"no package given and {latest} does not exist; run make refdata-build")
    return packages_dir() / latest.read_text(encoding="utf-8").strip()
