from __future__ import annotations

from typing import Any

from reconcile.compare_tracks import by_vessel, only_flink_dedup


def track(ts: int, lon: float, seq: int, offset: int, run: str = "r") -> dict[str, Any]:
    t: dict[str, Any] = dict.fromkeys(
        ("lat", "lon", "vx_ms", "vy_ms", "sog_kn", "cog_deg", "pos_sigma_m", "innovation_m", "innovation_nis", "dt_s"),
        0.0,
    )
    t.update(run_id=run, mmsi=1, event_ts=ts, meas_lat=54.0, meas_lon=lon, fix_seq=seq,
             src_partition=0, src_offset=offset)
    return t


def test_redelivered_tracks_count_once_and_other_runs_are_ignored() -> None:
    got = by_vessel("r", [track(1, 12.0, 1, 5), track(1, 12.0, 1, 5), track(1, 12.0, 1, 6, run="x")])
    assert len(got[1]) == 1


def test_same_second_repeat_within_a_metre_is_flink_dedup() -> None:
    a = by_vessel("r", [track(1, 12.422307, 1, 1), track(1, 12.42231, 2, 2), track(2, 12.4224, 3, 3)])[1]
    b = by_vessel("r", [track(1, 12.422307, 1, 1), track(2, 12.4224, 2, 2)])[1]
    assert only_flink_dedup(a, b)
    moved = by_vessel("r", [track(1, 12.422307, 1, 1), track(1, 12.4300, 2, 2)])[1]
    assert not only_flink_dedup(moved, b)  # 500 m apart: not a duplicate
    assert not only_flink_dedup(b, b)
