"""Download data sources listed in refdata/sources.yaml, verify their SHA-256, unzip them.

Usage:
    uv run python -m refdata.fetch --source dma_ais --day 2026-10-28 --dest data/ais
    uv run python -m refdata.fetch --incoming data/refdata/incoming \\
        --source eez --source wdpa_marine --source wpi

Behaviour:
    * Streams to <file>.part and resumes it with an HTTP Range request if present.
      The final filename only appears once the download is complete.
    * A complete file whose hash matches expected_sha256 is not downloaded again.
    * Empty expected_sha256: prints the computed hash so it can be committed, and
      exits 1. Nothing unpinned counts as fetched.
    * Hash mismatch: exits 1 without unzipping.
    * Unzips only after the hash check, then checks the source's expected members.
    * With --incoming, each source lives in <incoming>/<key>/<version>/<file>.
      A `manual: true` source is never downloaded: its url is the page a person
      downloads it from, and the file must already be there. If it is missing, or a
      non-manual download fails, fetch prints where to get the file and where to
      put it.
    * Every requested source is attempted, so one run reports everything missing.

Exit codes: 0 ok, 1 hash mismatch, download failure or manual step needed,
2 bad config or arguments.
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

from common.env import dotenv_value

CHUNK_SIZE = 1 << 20  # 1 MiB
DEFAULT_SOURCES = Path(__file__).with_name("sources.yaml")


class FetchError(Exception):
    """Download or verification failed (exit 1)."""


class ManualStepNeeded(FetchError):
    """The file has to be downloaded by hand and placed in the incoming directory (exit 1)."""


class ConfigError(Exception):
    """Bad sources.yaml entry or arguments (exit 2)."""


@dataclass(frozen=True)
class Source:
    key: str
    name: str
    url: str
    """Download URL (may contain {day}), or for a manual source the page to download it from."""
    version: str
    expected_sha256: str
    licence: str
    attribution: str
    file: str = ""
    """Name of the file. Required for manual sources; otherwise the last URL path segment."""
    manual: bool = False
    version_env: str = ""
    """Environment / .env key that selects the version (e.g. WDPA_VERSION)."""
    members: tuple[str, ...] = ()
    """File or directory names that must exist anywhere under the unzipped archive."""


_STR_FIELDS = ("name", "url", "version", "expected_sha256", "licence", "attribution", "file", "version_env")
_REQUIRED = ("name", "url", "version", "licence", "attribution")


def _entries(path: Path) -> dict[str, Any]:
    raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    sources = raw.get("sources") if isinstance(raw, dict) else None
    if not isinstance(sources, dict):
        raise ConfigError(f"{path} has no 'sources' mapping")
    return sources


def _parse(key: str, entry: Any, path: Path) -> Source:
    if not isinstance(entry, dict):
        raise ConfigError(f"source {key!r} in {path} is not a mapping")
    values: dict[str, str] = {}
    for field in _STR_FIELDS:
        value = entry.get(field, "" if field not in _REQUIRED else None)
        if not isinstance(value, str):
            raise ConfigError(f"source {key!r}: field {field!r} must be a string")
        values[field] = value.strip()
    manual = entry.get("manual", False)
    if not isinstance(manual, bool):
        raise ConfigError(f"source {key!r}: 'manual' must be true or false")
    members = entry.get("members", [])
    if not isinstance(members, list) or not all(isinstance(m, str) and m for m in members):
        raise ConfigError(f"source {key!r}: 'members' must be a list of file names")
    sha = values["expected_sha256"].lower()
    if sha and (len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha)):
        raise ConfigError(f"source {key!r}: expected_sha256 is not a hex SHA-256")
    values["expected_sha256"] = sha
    if not values["url"]:
        raise ConfigError(f"source {key!r} has no url")
    if manual and not values["file"]:
        raise ConfigError(f"manual source {key!r} needs 'file': the name exactly as downloaded")
    return Source(key=key, manual=manual, members=tuple(members), **values)


def load_source(path: Path, key: str) -> Source:
    sources = _entries(path)
    if key not in sources:
        raise ConfigError(f"source {key!r} not found in {path}")
    return _parse(key, sources[key], path)


def resolve_version(source: Source) -> str:
    """The pinned version, or the one set in source.version_env.

    A different version is only allowed while no hash is pinned: the hash belongs to
    the pinned version's bytes.
    """
    wanted = (dotenv_value(source.version_env) or "").strip() if source.version_env else ""
    if not wanted or wanted == source.version:
        return source.version
    if source.expected_sha256:
        raise ConfigError(
            f"{source.version_env}={wanted} but sources.yaml pins {source.key!r} at "
            f"{source.version}. Set {source.version_env}={source.version}, or update "
            "version and clear expected_sha256 to pin the new release."
        )
    return wanted


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
            # None of the sources is a web page. An HTML body is an error, bot-check or
            # terms page served with 200, and must never be hashed and pinned.
            if response.headers.get("content-type", "").startswith("text/html"):
                raise FetchError(f"GET {url} returned an HTML page, not the file")

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


def _fill(template: str, *, day: str = "", version: str = "") -> str:
    return template.replace("{day}", day).replace("{version}", version)


def _manual_steps(source: Source, url: str, target: Path, reason: str) -> ManualStepNeeded:
    lines = [f"{source.key}: {reason}. Download it by hand from", f"    {url}"]
    lines.append(f"  and save it as\n    {target}")
    if target.parent.is_dir():
        present = sorted(p.name for p in target.parent.iterdir() if p.is_file() and not p.name.endswith(".part"))
        if present:
            lines.append(f"  (that directory has {', '.join(present)}; the file name must match)")
    lines.append("  then rerun the fetch.")
    return ManualStepNeeded("\n".join(lines))


def verify(source: Source, target: Path) -> None:
    actual = sha256_file(target)
    if not source.expected_sha256:
        print(f"computed sha256 for {source.key} ({target.name}):")
        print(f'  expected_sha256: "{actual}"')
        raise FetchError(
            f"{source.key} is not pinned. Check the file came from the right place, then paste "
            f"the hash above into {DEFAULT_SOURCES} and rerun."
        )
    if actual != source.expected_sha256:
        raise FetchError(
            f"SHA-256 mismatch for {target}\n"
            f"  expected: {source.expected_sha256}\n"
            f"  actual:   {actual}\n"
            "The file is corrupt or the source changed. Delete it and rerun, "
            "or update expected_sha256 if the change is intended."
        )
    print(f"sha256 ok: {source.key} {actual}", file=sys.stderr)


def fetch_file(source: Source, url: str, target: Path, *, manual_fallback: bool) -> None:
    """Make sure target exists and matches source's hash, then unzip it next to itself."""
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        print(f"{target} exists, verifying hash", file=sys.stderr)
    elif source.manual:
        raise _manual_steps(source, url, target, "file not found")
    else:
        print(f"fetching {url}", file=sys.stderr)
        try:
            download(url, target)
        except (FetchError, httpx.HTTPError) as exc:
            if not manual_fallback:
                raise
            raise _manual_steps(source, url, target, f"download failed ({exc})") from exc

    verify(source, target)
    if zipfile.is_zipfile(target):
        unzip(target, target.parent)


def check_members(source: Source, root: Path) -> None:
    missing = [m for m in source.members if next(root.rglob(m), None) is None]
    if missing:
        raise FetchError(
            f"{source.key}: {', '.join(missing)} not found under {root} after unzip. "
            "The archive layout differs from sources.yaml 'members'."
        )


def fetch(source: Source, day: str | None, dest: Path) -> None:
    """Fetch a source straight into dest (the AIS day)."""
    if "{day}" in source.url:
        if day is None:
            raise ConfigError(f"source {source.key!r} needs --day")
        if source.expected_sha256 and day != source.version:
            raise ConfigError(
                f"expected_sha256 for {source.key!r} is for day {source.version}, not {day}. "
                "Fetch the pinned day, or update version and clear expected_sha256."
            )
    url = _fill(source.url, day=day or "")
    name = _fill(source.file, day=day or "") or PurePosixPath(httpx.URL(url).path).name
    fetch_file(source, url, dest / name, manual_fallback=False)
    check_members(source, dest)


def fetch_incoming(source: Source, incoming: Path) -> None:
    """Fetch a source into <incoming>/<key>/<version>/."""
    version = resolve_version(source)
    url = _fill(source.url, version=version)
    name = _fill(source.file, version=version) or PurePosixPath(httpx.URL(url).path).name
    target_dir = incoming / source.key / version
    fetch_file(source, url, target_dir / name, manual_fallback=True)
    check_members(source, target_dir)


def parse_day(value: str) -> str:
    try:
        return dt.date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {value!r}") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--source", action="append", required=True, help="key in sources.yaml, e.g. dma_ais (repeatable)"
    )
    parser.add_argument("--day", type=parse_day, help="YYYY-MM-DD, for sources with {day} in the url")
    where = parser.add_mutually_exclusive_group(required=True)
    where.add_argument("--dest", type=Path, help="output directory")
    where.add_argument("--incoming", type=Path, help="root of per-source <name>/<version>/ directories")
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES, help="path to sources.yaml")
    args = parser.parse_args(argv)

    status = 0
    for key in args.source:
        try:
            source = load_source(args.sources, key)
            if args.incoming is not None:
                fetch_incoming(source, args.incoming)
            else:
                fetch(source, args.day, args.dest)
        except ConfigError as exc:
            print(f"error: {exc}", file=sys.stderr)
            status = max(status, 2)
        except (FetchError, httpx.HTTPError, zipfile.BadZipFile) as exc:
            print(f"error: {exc}", file=sys.stderr)
            status = max(status, 1)
        else:
            print(f"ok: {key}", file=sys.stderr)
    return status


if __name__ == "__main__":
    sys.exit(main())
