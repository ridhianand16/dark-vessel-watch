from __future__ import annotations

import struct
from pathlib import Path

import pytest

from refdata.area import BBox
from refdata.build import version_of
from refdata.gpkg import GpkgError, split_blob
from refdata.package import MANIFEST, VerifyError, verify_package
from refdata.tests.fake_package import gpkg_blob, make_package, multipolygon, square
from refdata.tests.minisign_keys import SigningKey

BBOX = BBox(2.6909, 53.5224, 17.5334, 59.5686)


@pytest.fixture
def key() -> SigningKey:
    return SigningKey()


@pytest.fixture
def public_key(tmp_path: Path, key: SigningKey) -> Path:
    path = tmp_path / "dvw.pub"
    path.write_text(key.public_text())
    return path


def flip_byte(path: Path, offset: int = 100) -> None:
    data = bytearray(path.read_bytes())
    data[offset] ^= 0x01
    path.write_bytes(bytes(data))


def test_valid_package_verifies(tmp_path: Path, key: SigningKey, public_key: Path) -> None:
    pkg = verify_package(make_package(tmp_path / "p", key, BBOX), public_key)
    assert pkg.version == "zones-test00000001"
    assert pkg.bbox == BBOX
    assert (pkg.features("zones"), pkg.features("eez_boundaries")) == (4, 1)


def test_one_changed_byte_in_a_file_fails(tmp_path: Path, key: SigningKey, public_key: Path) -> None:
    pkg = make_package(tmp_path / "p", key, BBOX)
    flip_byte(pkg / "mpa.gpkg")
    with pytest.raises(VerifyError, match="mpa.gpkg: SHA-256"):
        verify_package(pkg, public_key)


def test_edited_manifest_fails_on_the_signature(tmp_path: Path, key: SigningKey, public_key: Path) -> None:
    pkg = make_package(tmp_path / "p", key, BBOX)
    manifest = pkg / MANIFEST
    manifest.write_text(manifest.read_text().replace('"features": 2', '"features": 3'))
    with pytest.raises(VerifyError, match="signature check failed"):
        verify_package(pkg, public_key)


def test_package_signed_by_another_key_fails(tmp_path: Path, public_key: Path) -> None:
    pkg = make_package(tmp_path / "p", SigningKey(), BBOX)
    with pytest.raises(VerifyError, match="not the trusted key"):
        verify_package(pkg, public_key)


def test_unlisted_or_missing_files_fail(tmp_path: Path, key: SigningKey, public_key: Path) -> None:
    pkg = make_package(tmp_path / "p", key, BBOX)
    (pkg / ".DS_Store").write_bytes(b"")  # dotfiles are ignored
    (pkg / "extra.gpkg").write_bytes(b"")
    with pytest.raises(VerifyError, match="not in the manifest: extra.gpkg"):
        verify_package(pkg, public_key)
    (pkg / "extra.gpkg").unlink()
    (pkg / "ports.gpkg").unlink()
    with pytest.raises(VerifyError, match="ports.gpkg is missing"):
        verify_package(pkg, public_key)


def test_renamed_package_directory_fails(tmp_path: Path, key: SigningKey, public_key: Path) -> None:
    pkg = make_package(tmp_path / "p", key, BBOX)
    with pytest.raises(VerifyError, match="holds package zones-test00000001"):
        verify_package(pkg.rename(pkg.with_name("zones-other")), public_key)


def test_version_depends_on_every_input_but_not_key_order() -> None:
    inputs = {"sources": {"eez": {"sha256": "a"}}, "port_radius_nm": 2.0}
    assert version_of(inputs) == version_of({"port_radius_nm": 2.0, "sources": {"eez": {"sha256": "a"}}})
    assert version_of(inputs) != version_of({**inputs, "port_radius_nm": 2.5})
    assert version_of(inputs).startswith("zones-") and len(version_of(inputs)) == 18


def test_gpkg_blob_header_is_stripped() -> None:
    wkb = multipolygon(square(10, 56))
    assert split_blob(gpkg_blob(wkb)) == (4326, wkb)
    # Big-endian header with an xy envelope (4 doubles) before the WKB.
    blob = b"GP\x00\x02" + struct.pack(">i", 3857) + struct.pack(">4d", 0, 1, 0, 1) + wkb
    assert split_blob(blob) == (3857, wkb)
    assert split_blob(b"GP\x00\x11" + struct.pack("<i", 4326)) == (4326, None)  # empty flag
    with pytest.raises(GpkgError):
        split_blob(wkb)
