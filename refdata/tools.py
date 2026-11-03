"""Run ogr2ogr and minisign in the refdata-tools container (GDAL + minisign, no network).

Each call is one `docker compose run --rm refdata-tools <command>` with only the
directories it needs mounted. The service has network_mode: none, so nothing the build
runs can download anything. The image is built once, while online:
`make refdata-image`.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from common.env import REPO_ROOT

SERVICE = "refdata-tools"
IMAGE = "dvw-refdata-tools:0.1.0"  # keep in step with deploy/compose/docker-compose.yml
GDAL_IMAGE = "ghcr.io/osgeo/gdal:ubuntu-small-3.13.1"  # keep in step with refdata/tools/Dockerfile
COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(REPO_ROOT / "deploy" / "compose" / "docker-compose.yml"),
    "--env-file",
    str(REPO_ROOT / ".env"),
]


class ToolError(Exception):
    pass


@dataclass(frozen=True)
class Mount:
    host: Path
    container: str
    read_only: bool = True

    def arg(self) -> str:
        return f"{self.host.resolve()}:{self.container}{':ro' if self.read_only else ''}"


def image_present() -> bool:
    result = subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True)
    return result.returncode == 0


def run(command: list[str], mounts: list[Mount], env: dict[str, str] | None = None) -> None:
    """Run command in the tools container. Raises ToolError with its output on failure."""
    if not image_present():
        raise ToolError(f"image {IMAGE} not found. Build it once while online: make refdata-image")
    args = [*COMPOSE, "run", "--rm", "--no-deps"]
    for m in mounts:
        args += ["-v", m.arg()]
    for key, value in (env or {}).items():
        args += ["-e", f"{key}={value}"]
    args += [SERVICE, *command]
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        raise ToolError(f"{command[0]} failed (exit {result.returncode}):\n{result.stderr.strip()}")
