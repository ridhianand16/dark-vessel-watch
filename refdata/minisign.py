"""Verify minisign signatures in Python, so verifying a package needs no external binary.

minisign (https://jedisct1.github.io/minisign/) signs with Ed25519. The files are text:

    public key   untrusted comment: ...
                 base64( "Ed" | key_id[8] | public_key[32] )

    signature    untrusted comment: ...
                 base64( algorithm[2] | key_id[8] | signature[64] )
                 trusted comment: <text>
                 base64( global_signature[64] )

algorithm "ED" signs BLAKE2b-512(file) (the default since minisign 0.8); "Ed" signs the
file itself. global_signature signs signature | trusted comment, so the trusted comment
is covered too. The untrusted comments are not. The build signs with the minisign
binary; refdata/tests/test_minisign.py checks this module against it.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
from dataclasses import dataclass

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

PREHASHED = b"ED"
LEGACY = b"Ed"
TRUSTED_PREFIX = "trusted comment: "


class SignatureError(Exception):
    pass


@dataclass(frozen=True)
class PublicKey:
    key_id: bytes
    key: Ed25519PublicKey


@dataclass(frozen=True)
class Signature:
    algorithm: bytes
    key_id: bytes
    signature: bytes
    trusted_comment: str
    global_signature: bytes


def _b64(line: str, length: int, what: str) -> bytes:
    try:
        raw = base64.b64decode(line.strip(), validate=True)
    except binascii.Error as exc:
        raise SignatureError(f"{what}: not base64") from exc
    if len(raw) != length:
        raise SignatureError(f"{what}: {len(raw)} bytes, expected {length}")
    return raw


def _lines(text: str, count: int, what: str) -> list[str]:
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) != count or not lines[0].startswith("untrusted comment:"):
        raise SignatureError(f"{what}: expected {count} lines starting with 'untrusted comment:'")
    return lines


def read_public_key(text: str) -> PublicKey:
    _, line = _lines(text, 2, "public key")
    raw = _b64(line, 42, "public key")
    if raw[:2] != LEGACY:
        raise SignatureError(f"public key: unknown algorithm {raw[:2]!r}")
    return PublicKey(key_id=raw[2:10], key=Ed25519PublicKey.from_public_bytes(raw[10:]))


def read_signature(text: str) -> Signature:
    _, sig_line, trusted_line, global_line = _lines(text, 4, "signature")
    raw = _b64(sig_line, 74, "signature")
    if raw[:2] not in (PREHASHED, LEGACY):
        raise SignatureError(f"signature: unknown algorithm {raw[:2]!r}")
    if not trusted_line.startswith(TRUSTED_PREFIX):
        raise SignatureError("signature: third line must start with 'trusted comment: '")
    return Signature(
        algorithm=raw[:2],
        key_id=raw[2:10],
        signature=raw[10:],
        trusted_comment=trusted_line[len(TRUSTED_PREFIX) :],
        global_signature=_b64(global_line, 64, "global signature"),
    )


def verify(message: bytes, signature: Signature, public_key: PublicKey) -> str:
    """Check signature over message. Returns the trusted comment; raises SignatureError."""
    if signature.key_id != public_key.key_id:
        raise SignatureError(
            f"signed with key {signature.key_id[::-1].hex().upper()}, "
            f"not the trusted key {public_key.key_id[::-1].hex().upper()}"
        )
    signed = hashlib.blake2b(message).digest() if signature.algorithm == PREHASHED else message
    try:
        public_key.key.verify(signature.signature, signed)
    except InvalidSignature as exc:
        raise SignatureError("signature does not match the file") from exc
    try:
        public_key.key.verify(signature.global_signature, signature.signature + signature.trusted_comment.encode())
    except InvalidSignature as exc:
        raise SignatureError("trusted comment was altered") from exc
    return signature.trusted_comment
