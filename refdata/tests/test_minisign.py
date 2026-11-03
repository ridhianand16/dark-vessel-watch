from __future__ import annotations

from pathlib import Path

import pytest

from refdata import tools
from refdata.minisign import SignatureError, read_public_key, read_signature, verify
from refdata.tests.minisign_keys import SigningKey

MESSAGE = b'{"package_version": "zones-test"}\n'


@pytest.mark.parametrize("prehash", [True, False])
def test_valid_signature_returns_trusted_comment(prehash: bool) -> None:
    key = SigningKey()
    sig = read_signature(key.sign(MESSAGE, "dvw zone package zones-test", prehash))
    assert verify(MESSAGE, sig, read_public_key(key.public_text())) == "dvw zone package zones-test"


def test_one_changed_byte_fails() -> None:
    key = SigningKey()
    sig = read_signature(key.sign(MESSAGE, "c"))
    with pytest.raises(SignatureError, match="does not match the file"):
        verify(MESSAGE.replace(b"test", b"tesT"), sig, read_public_key(key.public_text()))


def test_altered_trusted_comment_fails() -> None:
    key = SigningKey()
    text = key.sign(MESSAGE, "dvw zone package zones-test").replace("zones-test", "zones-other")
    with pytest.raises(SignatureError, match="trusted comment was altered"):
        verify(MESSAGE, read_signature(text), read_public_key(key.public_text()))


def test_other_key_fails() -> None:
    signer, trusted = SigningKey(), SigningKey()
    with pytest.raises(SignatureError, match="not the trusted key"):
        verify(MESSAGE, read_signature(signer.sign(MESSAGE, "c")), read_public_key(trusted.public_text()))
    # Same key id, different key: caught by the signature itself.
    trusted.key_id = signer.key_id
    with pytest.raises(SignatureError, match="does not match"):
        verify(MESSAGE, read_signature(signer.sign(MESSAGE, "c")), read_public_key(trusted.public_text()))


def test_malformed_files_are_rejected() -> None:
    with pytest.raises(SignatureError, match="expected 2 lines"):
        read_public_key("RWQ\n")
    with pytest.raises(SignatureError, match="not base64"):
        read_public_key("untrusted comment: x\n!!!\n")
    with pytest.raises(SignatureError, match="expected 4 lines"):
        read_signature("untrusted comment: x\nabc\n")


def test_matches_the_minisign_binary(tmp_path: Path) -> None:
    """A key and signature made by minisign itself verify here, and a tampered file does not."""
    if not tools.image_present():
        pytest.skip(f"{tools.IMAGE} not built (make refdata-image)")
    (tmp_path / "manifest.json").write_bytes(MESSAGE)
    mount = [tools.Mount(tmp_path, "/w", read_only=False)]
    tools.run(["minisign", "-G", "-W", "-p", "/w/k.pub", "-s", "/w/k.key"], mount)
    tools.run(["minisign", "-S", "-s", "/w/k.key", "-m", "/w/manifest.json", "-t", "dvw zone package zones-test"], mount)

    public_key = read_public_key((tmp_path / "k.pub").read_text())
    sig = read_signature((tmp_path / "manifest.json.minisig").read_text())
    assert sig.algorithm == b"ED"
    assert verify(MESSAGE, sig, public_key) == "dvw zone package zones-test"
    with pytest.raises(SignatureError):
        verify(MESSAGE + b" ", sig, public_key)
