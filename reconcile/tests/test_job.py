from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from reconcile.flink_check import tally
from reconcile.job import (
    COLUMNS,
    RUN_WIDE,
    Counts,
    Limits,
    evaluate,
    gather,
    latency_check,
    rows,
    tracks_match,
    wait_for_run,
)
from reconcile.run_check import Produced, RunError, Stored, compare

RUN = "20261104T101500Z-a1b2c3"
BASE = "20261104T090000Z-d4e5f6"
PRODUCED = [Produced(0, 100, 109, 10), Produced(3, 50, 52, 3)]
TAGS = {"zones_version": "zones-ee4167a002e7", "dist_eez_boundary_m": 9046.6}


def write_produced(tmp_path: Path, status: str) -> Path:
    path = tmp_path / RUN / "produced.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    doc: dict[str, Any] = {"run_id": RUN, "status": status}
    if status != "running":
        doc["partitions"] = {"0": {"first_offset": 100, "last_offset": 109, "count": 10}}
    path.write_text(json.dumps(doc))
    return path


def rec(partition: int, offset: int, **extra: Any) -> dict[str, Any]:
    return {"src_partition": partition, "src_offset": offset, "run_id": RUN, **TAGS, **extra}


def balanced() -> tuple[list[Any], list[Any], dict[int, Counts]]:
    """Partition 0: 7 enriched + 1 dlq + 2 dups; partition 3: 2 enriched + 1 dlq."""
    stored = compare(PRODUCED, {0: Stored(10, 0, 10), 3: Stored(3, 0, 3)})
    enriched = [rec(0, o) for o in range(100, 107)] + [rec(3, 50), rec(3, 51)]
    dlq = [rec(0, 107, reason="bad_mmsi"), rec(3, 52, reason="decode_error")]
    counts_msgs = [{"run_id": RUN, "src_partition": 0, "count": 2}]
    tallies, _ = tally(RUN, PRODUCED, enriched, dlq, counts_msgs)
    counts = gather(PRODUCED, stored, tallies, tracks={0: 6, 3: 2}, rollup={0: 6, 3: 2})
    return stored, tallies, counts


def by(checks: list[Any], name: str, partition: int) -> Any:
    (c,) = [c for c in checks if c.check == name and c.partition == partition]
    return c


# ---------------------------------------------------------------- run state


def test_complete_run_is_read_at_once(tmp_path: Path) -> None:
    path = write_produced(tmp_path, "complete")
    assert wait_for_run(path, RUN, time.monotonic()) == [Produced(0, 100, 109, 10)]


@pytest.mark.parametrize("status", ["interrupted", "failed"])
def test_unfinished_run_is_refused_without_waiting(tmp_path: Path, status: str) -> None:
    path = write_produced(tmp_path, status)
    start = time.monotonic()
    with pytest.raises(RunError, match=status):
        wait_for_run(path, RUN, start + 30)
    assert time.monotonic() - start < 1


def test_missing_run_is_refused(tmp_path: Path) -> None:
    with pytest.raises(RunError, match="not found"):
        wait_for_run(tmp_path / RUN / "produced.json", RUN, time.monotonic() + 30)


def test_running_run_is_waited_for(tmp_path: Path) -> None:
    path = write_produced(tmp_path, "running")
    finisher = threading.Timer(0.3, write_produced, (tmp_path, "complete"))
    finisher.start()
    try:
        assert wait_for_run(path, RUN, time.monotonic() + 10, poll_s=0.05)[0].count == 10
    finally:
        finisher.cancel()


def test_running_run_times_out(tmp_path: Path) -> None:
    path = write_produced(tmp_path, "running")
    with pytest.raises(RunError, match="still running"):
        wait_for_run(path, RUN, time.monotonic() + 0.1, poll_s=0.05)


# ----------------------------------------------------------------- checks


def test_balanced_run_passes_every_check() -> None:
    stored, tallies, counts = balanced()
    checks = evaluate(stored, tallies, counts, 0, Limits(dlq_warn=0.5))
    assert {c.status for c in checks} == {"ok"}
    # one row per partition and check
    assert sorted((c.check, c.partition) for c in checks) == sorted(
        (name, p) for name in ("raw_stored", "flink_balance", "zone_tagged", "rollup", "dlq_share") for p in (0, 3)
    )
    assert counts[0].dlq_reasons == {"bad_mmsi": 1}
    assert (counts[0].enriched, counts[0].dlq, counts[0].duplicates) == (7, 1, 2)


def test_each_imbalance_fails_its_own_check() -> None:
    stored, tallies, counts = balanced()
    stored = compare(PRODUCED, {0: Stored(9, 0, 9), 3: Stored(3, 0, 3)})
    tallies[1].untagged = 1
    tallies[1].enriched -= 1  # one enriched record lost on partition 3
    counts[0].rollup_fixes = 5
    checks = evaluate(stored, tallies, counts, 0, Limits(dlq_warn=0.5))
    assert by(checks, "raw_stored", 0).status == "fail"
    assert by(checks, "raw_stored", 0).note == "missing 1"
    assert by(checks, "flink_balance", 3).note == "missing 1"
    assert by(checks, "zone_tagged", 3).status == "fail"
    assert by(checks, "rollup", 0).note == "rollup 5 vs tracks 6"
    assert by(checks, "flink_balance", 0).status == "ok"


def test_offset_written_twice_fails_even_when_totals_match() -> None:
    stored, tallies, counts = balanced()
    tallies[0].repeated = 1
    checks = evaluate(stored, tallies, counts, 0, Limits(dlq_warn=0.5))
    assert by(checks, "flink_balance", 0).status == "fail"
    assert "1 offsets twice" in by(checks, "flink_balance", 0).note


def test_rollup_fixes_on_a_vessel_without_tracks_fail_once() -> None:
    stored, tallies, counts = balanced()
    checks = evaluate(stored, tallies, counts, 4, Limits(dlq_warn=0.5))
    assert by(checks, "rollup", 0).status == "fail"
    assert by(checks, "rollup", 3).status == "ok"


def test_dlq_above_the_share_warns_and_lists_reasons() -> None:
    stored, tallies, counts = balanced()
    checks = evaluate(stored, tallies, counts, 0, Limits(dlq_warn=0.2))
    assert by(checks, "dlq_share", 0).status == "ok"  # 1 of 10
    warn = by(checks, "dlq_share", 3)  # 1 of 3
    assert warn.status == "warn"
    assert warn.note == "33.3% in dlq (decode_error 1), above 20%"


# ---------------------------------------------------------------- latency


def test_latency_percentiles_and_warning() -> None:
    check, lat = latency_check([float(v) for v in range(1, 101)], Limits(latency_warn_ms=50))
    assert (lat.n, lat.p50, lat.p95, lat.p99) == (100, 51.0, 96.0, 100.0)
    assert check.partition == RUN_WIDE
    assert check.status == "warn"
    assert latency_check([10.0, 20.0], Limits())[0].status == "ok"
    assert latency_check([], Limits())[0].status == "warn"


# ----------------------------------------------------------- tracks match


def track(run: str, mmsi: int, partition: int, offset: int, lon: float) -> dict[str, Any]:
    t: dict[str, Any] = dict.fromkeys(
        ("lat", "lon", "vx_ms", "vy_ms", "sog_kn", "cog_deg", "pos_sigma_m", "innovation_m", "innovation_nis", "dt_s"),
        0.0,
    )
    t.update(run_id=run, mmsi=mmsi, event_ts=1, meas_lat=54.0, meas_lon=lon, fix_seq=1,
             src_partition=partition, src_offset=offset)
    return t


def test_tracks_match_per_partition() -> None:
    msgs = [
        track(BASE, 1, 0, 10, 12.0), track(RUN, 1, 0, 90, 12.0),  # same vessel, new offsets: match
        track(BASE, 2, 3, 11, 12.0), track(RUN, 2, 3, 91, 12.5),  # moved: differs
    ]
    checks = tracks_match(RUN, BASE, msgs)
    assert [(c.partition, c.status, c.expected, c.actual) for c in checks] == [(0, "ok", 1, 1), (3, "fail", 1, 0)]
    assert "1 vessels differ, first 2: track 1: meas_lon differ" in checks[1].note


# ------------------------------------------------------------------- rows


def test_rows_fit_the_table_and_carry_latency_only_run_wide() -> None:
    stored, tallies, counts = balanced()
    checks = evaluate(stored, tallies, counts, 0, Limits(dlq_warn=0.5))
    lat_check, lat = latency_check([5.0, 7.0], Limits())
    out = rows(RUN, counts, [*checks, lat_check], lat, datetime(2026, 11, 4, tzinfo=UTC))
    assert all(len(r) == len(COLUMNS) for r in out)
    named = [dict(zip(COLUMNS, r, strict=True)) for r in out]
    run_wide = [r for r in named if r["partition"] == RUN_WIDE]
    assert len(run_wide) == 1
    assert run_wide[0]["produced"] == 13 and run_wide[0]["latency_p99_ms"] == 7.0
    assert all(r["latency_p99_ms"] is None for r in named if r["partition"] != RUN_WIDE)
    assert next(r for r in named if r["partition"] == 0)["dlq_reasons"] == {"bad_mmsi": 1}
