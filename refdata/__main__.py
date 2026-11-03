"""Zone package commands.

    uv run python -m refdata build              # cut, sign; prints the package directory
    uv run python -m refdata verify [--package DIR]
    uv run python -m refdata load [--package DIR]
    uv run python -m refdata ensure [--package DIR]   # load only if none is active

Without --package, verify, load and ensure use the package the last build produced
(data/refdata/packages/LATEST). Exit codes: 0 ok, 1 build, verify or load failed,
3 (ensure only) no package is active and none has been built.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import psycopg

from refdata import tools
from refdata.build import BuildError, build, signing_key
from refdata.fetch import ConfigError, FetchError
from refdata.load import LoadReport, active_version, ensure, load
from refdata.package import PUBLIC_KEY, VerifyError, resolve, verify_package
from refdata.staging import StagingCheckFailed

EXIT_NO_PACKAGE = 3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="refdata", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["build", "verify", "load", "ensure"])
    parser.add_argument("--package", type=Path, help="package directory (default: the last build)")
    parser.add_argument("--public-key", type=Path, default=PUBLIC_KEY)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s", stream=sys.stderr)

    try:
        if args.command == "build":
            print(build(signing_key(), args.public_key))
        elif args.command == "verify":
            pkg = verify_package(resolve(args.package), args.public_key)
            print(f"verify: {pkg.version} OK: signature, manifest and {len(pkg.files)} files "
                  f"(manifest sha256 {pkg.manifest_sha256})")
        elif args.command == "load":
            print_load(load(resolve(args.package), args.public_key))
        else:
            active = active_version()
            if active is not None:
                print(f"ensure: {active} is active; nothing to load")
                return 0
            try:
                package = resolve(args.package)
            except VerifyError as exc:
                print(f"refdata ensure: no zone package is active and none is built ({exc}). "
                      "Run make fetch-refdata and make refdata.", file=sys.stderr)
                return EXIT_NO_PACKAGE
            report = ensure(package, args.public_key)
            if report.loaded is not None:
                print_load(report.loaded)
    except (BuildError, VerifyError, StagingCheckFailed, FetchError, ConfigError, tools.ToolError) as exc:
        print(f"refdata {args.command}: FAILED: {exc}", file=sys.stderr)
        return 1
    except psycopg.Error as exc:
        print(f"refdata {args.command}: FAILED (database, nothing changed): {exc}", file=sys.stderr)
        return 1
    return 0


def print_load(r: LoadReport) -> None:
    rows = ", ".join(f"{t} {n:,}" for t, n in r.rows.items())
    print(f"load: {r.version} live: {rows}. verify {r.verify_s:.1f} s, "
          f"staging load and checks {r.load_s:.1f} s, swap {r.swap_ms:.0f} ms")


if __name__ == "__main__":
    sys.exit(main())
