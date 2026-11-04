"""What a report was measured on: code, machine, Docker VM, images and data.

Everything is read, never assumed: a field that cannot be read says "unknown" rather
than failing the report.
"""

from __future__ import annotations

import json
import platform
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime

from common.env import REPO_ROOT, setting
from report.sampler import CONTAINER_PREFIX

UNKNOWN = "unknown"


def _run(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True, cwd=REPO_ROOT).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


@dataclass(frozen=True)
class Image:
    container: str
    tag: str  # the image the container was created from, as named
    image_id: str  # short id of the image the container actually runs
    current: bool  # whether the tag still points at that image (False: rebuilt since)


@dataclass(frozen=True)
class Environment:
    started_at: datetime
    git_sha: str
    git_short: str
    git_dirty: bool
    host: str
    host_os: str
    docker: str
    images: tuple[Image, ...]
    ais_day: str
    zone_package: str


def git() -> tuple[str, str, bool]:
    sha = _run("git", "rev-parse", "HEAD") or UNKNOWN
    short = _run("git", "rev-parse", "--short", "HEAD") or UNKNOWN
    # Untracked files count: they could be code the run used.
    dirty = bool(_run("git", "status", "--porcelain"))
    return sha, short, dirty


def host() -> tuple[str, str]:
    if platform.system() == "Darwin":
        model = _run("sysctl", "-n", "hw.model") or UNKNOWN
        cpu = _run("sysctl", "-n", "machdep.cpu.brand_string") or UNKNOWN
        cores = _run("sysctl", "-n", "hw.ncpu") or UNKNOWN
        mem = _run("sysctl", "-n", "hw.memsize")
        gib = f"{int(mem) / 1024**3:.0f} GB" if mem.isdigit() else UNKNOWN
        return f"{model}, {cpu}, {cores} cores, {gib} RAM", f"macOS {_run('sw_vers', '-productVersion') or UNKNOWN}"
    return f"{platform.machine()}, {platform.processor() or UNKNOWN}", f"{platform.system()} {platform.release()}"


def docker_vm() -> str:
    raw = _run("docker", "info", "--format", "{{json .}}")
    try:
        info = json.loads(raw)
    except ValueError:
        return UNKNOWN
    mem = int(info.get("MemTotal", 0)) / 1024**3
    return (f"{info.get('OperatingSystem', UNKNOWN)}, Docker {info.get('ServerVersion', UNKNOWN)}, "
            f"{info.get('NCPU', UNKNOWN)} CPUs, {mem:.1f} GiB, kernel {info.get('KernelVersion', UNKNOWN)}")


def images() -> tuple[Image, ...]:
    names = _run("docker", "ps", "--filter", f"name={CONTAINER_PREFIX}", "--format", "{{.Names}}").split()
    out = []
    for name in sorted(names):
        tag, _, image_id = _run("docker", "inspect", "-f", "{{.Config.Image}}|{{.Image}}", name).partition("|")
        tag_id = _run("docker", "image", "inspect", "-f", "{{.Id}}", tag) if tag else ""
        out.append(Image(name, tag or UNKNOWN, image_id.removeprefix("sha256:")[:12] or UNKNOWN,
                         bool(tag_id) and tag_id == image_id))
    return tuple(out)


def zone_package() -> str:
    from refdata.load import active_version  # psycopg import only when needed

    try:
        return active_version() or "none active"
    except Exception as e:  # recorded in the report, not fatal
        return f"{UNKNOWN} ({type(e).__name__})"


def collect() -> Environment:
    sha, short, dirty = git()
    host_desc, host_os = host()
    return Environment(
        started_at=datetime.now(UTC),
        git_sha=sha,
        git_short=short,
        git_dirty=dirty,
        host=host_desc,
        host_os=host_os,
        docker=docker_vm(),
        images=images(),
        ais_day=setting("AIS_DAY", UNKNOWN),
        zone_package=zone_package(),
    )
