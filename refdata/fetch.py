"""Download a data source listed in refdata/sources.yaml, verify its SHA-256, unzip it.

Usage:
    uv run python refdata/fetch.py --source dma_ais --day 2026-10-28 --dest data/ais

Behaviour:
    * Streams to <file>.part and resumes it with an HTTP Range request if present.
      The final filename only appears once the download is complete.
    * A complete file whose hash matches expected_sha256 is not downloaded again.
    * Empty expected_sha256: prints the computed hash so it can be committed.
    * Hash mismatch: exits 1 without unzipping.
    * Unzips only after the hash check.

Exit codes: 0 ok, 1 hash mismatch or download failure, 2 bad config or arguments.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import httpx
import yaml

CHUNK_SIZE = 1 << 20  # 1 MiB
DEFAULT_SOURCES = Path(__file__).with_name("sources.yaml")


class FetchError(Exception):
    """Download or verification failed (exit 1)."""


class ConfigError(Exception):
    """Bad sources.yaml entry or arguments (exit 2)."""


@dataclass(frozen=True)
class Source:
    key: str
    name: str
    url: str
    version: str
    expected_sha256: str
    licence: str
    attribution: str


def load_source(path: Path, key: str) -> Source:
    raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    sources = raw.get("sources") if isinstance(raw, dict) else None
    if not isinstance(sources, dict) or key not in sources:
        raise ConfigError(f"source {key!r} not found in {path}")
    entry = sources[key]
    if not isinstance(entry, dict):
        raise ConfigError(f"source {key!r} in {path} is not a mapping")
    fields = ("name", "url", "version", "expected_sha256", "licence", "attribution")
    values: dict[str, str] = {}
    for field in fields:
        value = entry.get(field)
        if not isinstance(value, str):
            raise ConfigError(f"source {key!r}: field {field!r} must be a string")
        values[field] = value.strip()
    if not values["url"]:
        raise ConfigError(f"source {key!r} has no url (not implemented yet)")
    sha = values["expected_sha256"].lower()
    if sha and (len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha)):
        raise ConfigError(f"source {key!r}: expected_sha256 is not a hex SHA-256")
    values["expected_sha256"] = sha
    return Source(key=key, **values)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _total_size(response: httpx.Response, offset: int) -> int | None:
    """Full file size from Content-Range (206) or Content-Length (200)."""
    content_range = response.headers.get("content-range")
    if content_range and "/" in content_range:
        total = content_range.rsplit("/", 1)[1]
        return int(total) if total.isdigit() else None
    length = response.headers.get("content-length")
    return offset + int(length) if length and length.isdigit() else None


def download(url: str, target: Path) -> None:
    """Stream url to target via target.part, resuming a partial download."""
    part = target.with_name(target.name + ".part")
    offset = part.stat().st_size if part.exists() else 0
    headers = {"Range": f"bytes={offset}-"} if offset else {}

    timeout = httpx.Timeout(30.0, read=120.0)
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        with client.stream("GET", url, headers=headers) as response:
            if response.status_code == 416 and offset:
                # Range starts at or past EOF: the .part is already complete.
                print(f"resume: {part.name} already complete ({offset} bytes)", file=sys.stderr)
                part.replace(target)
                return
            if response.status_code == 206:
                content_range = response.headers.get("content-range", "")
                if not content_range.startswith(f"bytes {offset}-"):
                    raise FetchError(f"unexpected Content-Range {content_range!r} for offset {offset}")
                mode = "ab"
                print(f"resume: continuing {part.name} from byte {offset}", file=sys.stderr)
            elif response.status_code == 200:
                if offset:
                    print("resume: server ignored Range, restarting download", file=sys.stderr)
                offset = 0
                mode = "wb"
            else:
                raise FetchError(f"GET {url} returned HTTP {response.status_code}")

            total = _total_size(response, offset)
            written = offset
            last_report = 0.0
            with part.open(mode) as f:
                for chunk in response.iter_bytes(CHUNK_SIZE):
                    f.write(chunk)
                    written += len(chunk)
                    now = time.monotonic()
                    if now - last_report >= 2.0:
                        last_report = now
                        pct = f" ({100 * written / total:.1f}%)" if total else ""
                        print(f"  {written / 1e6:,.0f} MB{pct}", file=sys.stderr)

    if total is not None and written != total:
        raise FetchError(f"incomplete download: got {written} of {total} bytes; rerun to resume")
    part.replace(target)
    print(f"downloaded {target} ({written} bytes)", file=sys.stderr)


def unzip(archive: Path, dest: Path) -> list[Path]:
    """Extract archive into dest, rejecting unsafe member paths. Skips members already extracted."""
    extracted: list[Path] = []
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            member = PurePosixPath(info.filename)
            if member.is_absolute() or ".." in member.parts:
                raise FetchError(f"refusing unsafe path in zip: {info.filename!r}")
            out = dest.joinpath(*member.parts)
            if out.exists() and out.stat().st_size == info.file_size:
                print(f"skip unzip: {out} already extracted", file=sys.stderr)
                extracted.append(out)
                continue
            out.parent.mkdir(parents=True, exist_ok=True)
            tmp = out.with_name(out.name + ".part")
            with zf.open(info) as src, tmp.open("wb") as dst:
                while chunk := src.read(CHUNK_SIZE):
                    dst.write(chunk)
            tmp.replace(out)
            print(f"unzipped {out} ({info.file_size} bytes)", file=sys.stderr)
            extracted.append(out)
    return extracted


def fetch(source: Source, day: str | None, dest: Path) -> None:
    if "{day}" in source.url:
        if day is None:
            raise ConfigError(f"source {source.key!r} needs --day")
        if source.expected_sha256 and day != source.version:
            raise ConfigError(
                f"expected_sha256 for {source.key!r} is for day {source.version}, not {day}. "
                "Fetch the pinned day, or update version and clear expected_sha256."
            )
        url = source.url.format(day=day)
    else:
        url = source.url

    dest.mkdir(parents=True, exist_ok=True)
    target = dest / PurePosixPath(httpx.URL(url).path).name

    if target.exists():
        print(f"{target} exists, verifying hash", file=sys.stderr)
    else:
        print(f"fetching {url}", file=sys.stderr)
        download(url, target)

    actual = sha256_file(target)
    if not source.expected_sha256:
        print(f"computed sha256 for {source.key} ({target.name}):")
        print(f"  expected_sha256: \"{actual}\"")
        print(f"paste it into {DEFAULT_SOURCES} to pin this file", file=sys.stderr)
    elif actual != source.expected_sha256:
        raise FetchError(
            f"SHA-256 mismatch for {target}\n"
            f"  expected: {source.expected_sha256}\n"
            f"  actual:   {actual}\n"
            "The file is corrupt or the source changed. Delete it and rerun, "
            "or update expected_sha256 if the change is intended."
        )
    else:
        print(f"sha256 ok: {actual}", file=sys.stderr)

    if zipfile.is_zipfile(target):
        unzip(target, dest)


def parse_day(value: str) -> str:
    try:
        return dt.date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {value!r}") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, help="key in sources.yaml, e.g. dma_ais")
    parser.add_argument("--day", type=parse_day, help="YYYY-MM-DD, for sources with {day} in the url")
    parser.add_argument("--dest", type=Path, required=True, help="output directory")
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES, help="path to sources.yaml")
    args = parser.parse_args(argv)

    try:
        source = load_source(args.sources, args.source)
        fetch(source, args.day, args.dest)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (FetchError, httpx.HTTPError, zipfile.BadZipFile) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
