"""Compare the ais.tracks of two replays of the same rows, vessel by vessel.

    uv run python -m reconcile.compare_tracks <run_a> <run_b>   # or: make compare-tracks A=<run> B=<run>

Used to check that moving the tracker from ais.raw to ais.enriched changed no track.
Two runs of the same file and --limit carry the same reports, but every run has its
own run_id, offsets, trace_ids and send times, so a track message is compared on what
the tracker computes: per MMSI, the sequence of (event_ts, measured position, filtered
state, innovation, dt_s, fix_seq). Redelivered copies (the same src_partition and
src_offset twice, ADR 0004) count once.

A vessel is identical when both sequences are equal; otherwise the first difference is
printed. One difference is expected and labelled "flink dedup": the Flink job's
duplicate key rounds positions to 5 decimals (~1 m, ADR 0015), while the tracker's own
check needs an exact repeat. A second report in the same second within a metre is
dropped by Flink but was a zero-dt update for the tracker on ais.raw, which changes
that vessel's later filter output slightly. A difference is labelled so when the
measured fixes of run_b are those of run_a minus such repeats.

Exit codes: 0 every vessel identical or differing only by flink dedup, 1 any other
difference, 2 a run cannot be read.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from common.env import REPO_ROOT, setting
from reconcile.kafka import SEEK_MARGIN_MS, read_topic, run_started_ms
from reconcile.run_check import RunError

TRACKS = "ais.tracks"
COMPARED = (
    "event_ts", "meas_lat", "meas_lon", "lat", "lon", "vx_ms", "vy_ms", "sog_kn", "cog_deg",
    "pos_sigma_m", "innovation_m", "innovation_nis", "dt_s", "fix_seq",
)
Track = tuple[Any, ...]


def by_vessel(run_id: str, tracks: Iterable[dict[str, Any]]) -> dict[int, list[Track]]:
    seen: set[tuple[int, int]] = set()
    out: dict[int, list[Track]] = defaultdict(list)
    for t in tracks:
        if t.get("run_id") != run_id:
            continue
        src = (t["src_partition"], t["src_offset"])
        if src in seen:
            continue
        seen.add(src)
        out[t["mmsi"]].append(tuple(t[k] for k in COMPARED))
    for seq in out.values():
        seq.sort(key=lambda r: (r[0], r[-1]))  # event_ts, fix_seq: per-vessel order
    return dict(out)


def first_difference(a: list[Track], b: list[Track]) -> str:
    for i, (x, y) in enumerate(zip(a, b, strict=False)):
        if x != y:
            fields = [k for k, u, v in zip(COMPARED, x, y, strict=True) if u != v]
            return f"track {i + 1}: {', '.join(fields)} differ (event_ts {x[0]} vs {y[0]})"
    return f"same first {min(len(a), len(b))} tracks, then {len(a)} vs {len(b)} in total"


def dedup_key(t: Track) -> tuple[Any, ...]:
    """Flink's duplicate key without the MMSI: event_ts and position to 5 decimals."""
    return (t[0], round(t[1], 5), round(t[2], 5))


def only_flink_dedup(a: list[Track], b: list[Track]) -> bool:
    """b's measured fixes are a's, minus repeats of an earlier fix under Flink's key."""
    fixes_b = [t[:3] for t in b]
    kept: list[tuple[Any, ...]] = []
    keys: set[tuple[Any, ...]] = set()
    for t in a:
        k = dedup_key(t)
        if k not in keys:
            kept.append(t[:3])
            keys.add(k)
    return kept == fixes_b and len(a) > len(b)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="compare_tracks", description=__doc__.splitlines()[0])
    ap.add_argument("run_a")
    ap.add_argument("run_b")
    ap.add_argument("--runs-dir", type=Path, default=REPO_ROOT / "data" / "runs")
    args = ap.parse_args(argv)
    brokers = setting("REDPANDA_BROKERS", "localhost:19092")
    try:
        starts = [run_started_ms(args.runs_dir / r / "produced.json") for r in (args.run_a, args.run_b)]
        since = min(starts) - SEEK_MARGIN_MS
        tracks = list(read_topic(brokers, TRACKS, since))
    except (RunError, FileNotFoundError) as e:
        print(f"compare_tracks: {e}", file=sys.stderr)
        return 2
    a, b = by_vessel(args.run_a, tracks), by_vessel(args.run_b, tracks)
    same = [m for m in a.keys() & b.keys() if a[m] == b[m]]
    differ = sorted(m for m in a.keys() | b.keys() if a.get(m) != b.get(m))
    print(f"{args.run_a}: {sum(map(len, a.values()))} tracks, {len(a)} vessels")
    print(f"{args.run_b}: {sum(map(len, b.values()))} tracks, {len(b)} vessels")
    dedup = [m for m in differ if only_flink_dedup(a.get(m, []), b.get(m, []))]
    print(f"identical vessels: {len(same)}, differing: {len(differ)} ({len(dedup)} by flink dedup)")
    for m in differ:
        why = first_difference(a.get(m, []), b.get(m, []))
        print(f"  {m}: {why}" + ("; flink dedup" if m in dedup else ""))
    if len(dedup) < len(differ):
        print("compare_tracks: DIFFERENT", file=sys.stderr)
        return 1
    print(f"compare_tracks: OK, identical tracks except {len(dedup)} vessels with same-second repeats Flink deduped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
