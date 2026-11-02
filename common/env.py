"""Read settings from the environment first, then the repo's .env file."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def dotenv_value(key: str, path: Path = REPO_ROOT / ".env") -> str | None:
    """Environment first, then .env. Minimal KEY=VALUE reader; no python-dotenv dependency."""
    if key in os.environ:
        return os.environ[key]
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    for line in lines:
        k, sep, v = line.partition("=")
        if sep and k.strip() == key:
            return v.strip().strip('"').strip("'")
    return None


def setting(key: str, default: str) -> str:
    """dotenv_value() with a default for unset or empty values."""
    return dotenv_value(key) or default
