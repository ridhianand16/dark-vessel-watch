"""The build reads its sources from archives it extracts after their hash check, never from
what an earlier unzip left in incoming/. GDAL is not run: ogr_cut is replaced by a probe."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest
import yaml

from refdata import build, gpkg
from refdata.area import BBox
from refdata.fetch import FetchError

GOOD, TAMPERED = b"GOOD", b"EVIL"  # same size: fetch's unzip would skip re-extracting it


class Stop(Exception):
    pass


def zip_bytes(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def source(file: str, data: bytes, version: str) -> dict[str, object]:
    return {
        "name": file, "url": "https://example.invalid/", "version": version, "manual": True, "file": file,
        "licence": "test", "attribution": "test", "expected_sha256": hashlib.sha256(data).hexdigest(),
    }  # fmt: skip


@pytest.fixture
def incoming(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "refdata"
    eez = zip_bytes({"World_EEZ_v12_gpkg/eez_v12.gpkg": GOOD, "World_EEZ_v12_gpkg/eez_boundaries_v12.gpkg": GOOD})
    wdpa = zip_bytes({"WDPA_WDOECM_Nov2026_Public_marine.gdb/a00000001.gdbtable": GOOD})
    wpi = json.dumps({"type": "FeatureCollection", "features": [{
        "type": "Feature", "geometry": {"type": "Point", "coordinates": [12.6, 55.7]},
        "properties": {"INDEX_NO": 29230, "PORT_NAME": "KOBENHAVN", "COUNTRY": "DK"},
    }]}).encode()  # fmt: skip
    files = {
        ("eez", "v12", "eez.zip"): eez,
        ("wdpa_marine", "Nov2026", "wdpa.zip"): wdpa,
        ("wpi", "2019", "wpi.geojson"): wpi,
    }
    for (key, version, name), data in files.items():
        (root / "incoming" / key / version).mkdir(parents=True)
        (root / "incoming" / key / version / name).write_bytes(data)
    # What an earlier unzip left behind, edited since. The build must not read it.
    stale = root / "incoming" / "eez" / "v12" / "World_EEZ_v12_gpkg" / "eez_v12.gpkg"
    stale.parent.mkdir()
    stale.write_bytes(TAMPERED)
    sources = {"sources": {key: source(name, data, version) for (key, version, name), data in files.items()}}
    (tmp_path / "sources.yaml").write_text(yaml.safe_dump(sources))
    monkeypatch.setattr(build, "SOURCES", tmp_path / "sources.yaml")
    monkeypatch.setenv("REFDATA_DIR", str(root))
    return root


def test_layers_are_cut_from_archives_extracted_after_the_hash_check(
    incoming: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    read: dict[str, bytes] = {}

    def probe(cut: build.Cut, src: str, bbox: BBox, build_dir: Path) -> None:
        assert src.startswith("/build/work/src/"), src
        host = build_dir / src.removeprefix("/build/")
        f = host if host.is_file() else next(p for p in host.rglob("*") if p.is_file())
        read[cut.layer.name] = f.read_bytes()
        if cut.layer.name == "mpa":  # both zip sources seen
            raise Stop
        (build_dir / f"{cut.layer.name}.gpkg").write_bytes(b"")  # stands in for GDAL's output

    monkeypatch.setattr(build, "ogr_cut", probe)
    monkeypatch.setattr(gpkg, "geometry_column", lambda path, layer: ("geom", "MULTIPOLYGON", 4326))
    monkeypatch.setattr(gpkg, "count", lambda path, layer: 1)
    key = tmp_path / "dvw.key"
    key.write_text("unused")
    with pytest.raises(Stop):
        build.build(key)
    assert read == {"eez": GOOD, "mpa": GOOD}
    assert not list((incoming / "packages").glob(".*.tmp"))  # the work directory is cleaned up


def test_a_source_that_fails_its_hash_is_never_extracted(incoming: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (incoming / "incoming" / "eez" / "v12" / "eez.zip").write_bytes(zip_bytes({"x/eez_v12.gpkg": TAMPERED}))
    monkeypatch.setattr(build, "ogr_cut", lambda *a: pytest.fail("cut a source that failed its hash"))
    with pytest.raises(FetchError, match="SHA-256 mismatch"):
        build.build(incoming / "unused.key")
