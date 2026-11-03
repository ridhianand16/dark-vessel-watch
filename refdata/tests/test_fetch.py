from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

import pytest

from refdata.fetch import main

SHA_OF_EMPTY = hashlib.sha256(b"").hexdigest()


def make_zip(path: Path, members: dict[str, bytes]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_sources(path: Path, eez_sha: str, wdpa_sha: str = "") -> Path:
    path.write_text(
        f"""
sources:
  eez:
    name: EEZ polygons and boundary lines
    url: https://www.marineregions.org/downloads.php
    file: World_EEZ_v12.zip
    manual: true
    version: v12
    expected_sha256: "{eez_sha}"
    licence: CC BY-NC-SA 4.0
    attribution: VLIZ
    members: [eez_v12.gpkg, eez_boundaries_v12.gpkg]
  wdpa_marine:
    name: WDPA marine
    url: https://www.protectedplanet.net/
    file: WDPA_{{version}}_marine.zip
    manual: true
    version: Nov2026
    version_env: DVW_TEST_WDPA_VERSION
    expected_sha256: "{wdpa_sha}"
    licence: no redistribution
    attribution: UNEP-WCMC and IUCN
""",
        encoding="utf-8",
    )
    return path


def run(sources: Path, incoming: Path, *keys: str) -> int:
    argv = ["--sources", str(sources), "--incoming", str(incoming)]
    for key in keys:
        argv += ["--source", key]
    return main(argv)


def test_missing_manual_file_prints_where_to_put_it(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sources = write_sources(tmp_path / "sources.yaml", SHA_OF_EMPTY)
    assert run(sources, tmp_path / "incoming", "eez") == 1
    err = capsys.readouterr().err
    assert "eez: file not found" in err
    assert "https://www.marineregions.org/downloads.php" in err
    assert str(tmp_path / "incoming" / "eez" / "v12" / "World_EEZ_v12.zip") in err


def test_manual_file_in_place_verifies_unzips_and_finds_members(tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    archive = incoming / "eez" / "v12" / "World_EEZ_v12.zip"
    sha = make_zip(archive, {"World_EEZ_v12/eez_v12.gpkg": b"p", "World_EEZ_v12/eez_boundaries_v12.gpkg": b"l"})
    sources = write_sources(tmp_path / "sources.yaml", sha)
    assert run(sources, incoming, "eez") == 0
    assert (archive.parent / "World_EEZ_v12" / "eez_boundaries_v12.gpkg").read_bytes() == b"l"


def test_corrupt_file_fails_without_unzipping(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    incoming = tmp_path / "incoming"
    archive = incoming / "eez" / "v12" / "World_EEZ_v12.zip"
    sha = make_zip(archive, {"eez_v12.gpkg": b"p"})
    with archive.open("ab") as f:
        f.write(b"\0")
    sources = write_sources(tmp_path / "sources.yaml", sha)
    assert run(sources, incoming, "eez") == 1
    assert "SHA-256 mismatch" in capsys.readouterr().err
    assert not (archive.parent / "eez_v12.gpkg").exists()


def test_member_missing_from_archive_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    incoming = tmp_path / "incoming"
    sha = make_zip(incoming / "eez" / "v12" / "World_EEZ_v12.zip", {"eez_v12.gpkg": b"p"})
    sources = write_sources(tmp_path / "sources.yaml", sha)
    assert run(sources, incoming, "eez") == 1
    assert "eez_boundaries_v12.gpkg not found" in capsys.readouterr().err


def test_unpinned_file_prints_hash_and_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    incoming = tmp_path / "incoming"
    sha = make_zip(incoming / "eez" / "v12" / "World_EEZ_v12.zip", {"eez_v12.gpkg": b"p"})
    sources = write_sources(tmp_path / "sources.yaml", "")
    assert run(sources, incoming, "eez") == 1
    out = capsys.readouterr()
    assert f'expected_sha256: "{sha}"' in out.out
    assert "not pinned" in out.err


def test_every_source_is_attempted(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sources = write_sources(tmp_path / "sources.yaml", SHA_OF_EMPTY, SHA_OF_EMPTY)
    assert run(sources, tmp_path / "incoming", "eez", "wdpa_marine") == 1
    err = capsys.readouterr().err
    assert "eez: file not found" in err
    assert "wdpa_marine: file not found" in err
    assert str(tmp_path / "incoming" / "wdpa_marine" / "Nov2026" / "WDPA_Nov2026_marine.zip") in err


def test_version_env_must_match_pinned_version(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DVW_TEST_WDPA_VERSION", "Dec2026")
    sources = write_sources(tmp_path / "sources.yaml", SHA_OF_EMPTY, SHA_OF_EMPTY)
    assert run(sources, tmp_path / "incoming", "wdpa_marine") == 2
    assert "pins 'wdpa_marine' at Nov2026" in capsys.readouterr().err


def test_version_env_picks_the_directory_while_unpinned(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DVW_TEST_WDPA_VERSION", "Dec2026")
    sources = write_sources(tmp_path / "sources.yaml", SHA_OF_EMPTY, "")
    assert run(sources, tmp_path / "incoming", "wdpa_marine") == 1
    assert str(tmp_path / "incoming" / "wdpa_marine" / "Dec2026" / "WDPA_Dec2026_marine.zip") in capsys.readouterr().err


def test_real_sources_yaml_parses() -> None:
    from refdata.fetch import DEFAULT_SOURCES, _entries, _parse

    sources = [_parse(key, entry, DEFAULT_SOURCES) for key, entry in _entries(DEFAULT_SOURCES).items()]
    assert all(s.expected_sha256 for s in sources), "every source must be pinned"



def test_manual_source_needs_a_file_name(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sources = tmp_path / "sources.yaml"
    sources.write_text(
        "sources:\n  wpi:\n    name: WPI\n    url: https://example.invalid/\n    version: v1\n"
        "    manual: true\n    expected_sha256: ''\n    licence: pd\n    attribution: NGA\n",
        encoding="utf-8",
    )
    assert run(sources, tmp_path / "incoming", "wpi") == 2
    assert "needs 'file'" in capsys.readouterr().err
