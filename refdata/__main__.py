"""Zone package commands.

    uv run python -m refdata build              # cut, sign; prints the package directory
    uv run python -m refdata verify [--package DIR]
    uv run python -m refdata load [--package DIR]

Without --package, verify and load use the package the last build produced
(data/refdata/packages/LATEST). Exit codes: 0 ok, 1 build, verify or load failed.
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
from refdata.load import load
from refdata.package import PUBLIC_KEY, VerifyError, resolve, verify_package
from refdata.staging import StagingCheckFailed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="refdata", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["build", "verify", "load"])
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
        else:
            r = load(resolve(args.package), args.public_key)
            rows = ", ".join(f"{t} {n:,}" for t, n in r.rows.items())
            print(f"load: {r.version} live: {rows}. verify {r.verify_s:.1f} s, "
                  f"staging load and checks {r.load_s:.1f} s, swap {r.swap_ms:.0f} ms")
    except (BuildError, VerifyError, StagingCheckFailed, FetchError, ConfigError, tools.ToolError) as exc:
        print(f"refdata {args.command}: FAILED: {exc}", file=sys.stderr)
        return 1
    except psycopg.Error as exc:
        print(f"refdata {args.command}: FAILED (database, nothing changed): {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
