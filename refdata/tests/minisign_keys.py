"""Make minisign-format keys and signatures in Python, for tests only."""

from __future__ import annotations

import base64
import hashlib
import os

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat


class SigningKey:
    def __init__(self) -> None:
        self.private = Ed25519PrivateKey.generate()
        self.key_id = os.urandom(8)

    def public_text(self) -> str:
        raw = self.private.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        return "untrusted comment: test key\n" + base64.b64encode(b"Ed" + self.key_id + raw).decode() + "\n"

    def sign(self, message: bytes, trusted_comment: str, prehash: bool = True) -> str:
        algorithm = b"ED" if prehash else b"Ed"
        signature = self.private.sign(hashlib.blake2b(message).digest() if prehash else message)
        global_signature = self.private.sign(signature + trusted_comment.encode())
        return (
            "untrusted comment: test signature\n"
            + base64.b64encode(algorithm + self.key_id + signature).decode()
            + f"\ntrusted comment: {trusted_comment}\n"
            + base64.b64encode(global_signature).decode()
            + "\n"
        )
