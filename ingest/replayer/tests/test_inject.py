from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any

import pytest

from ingest.replayer import inject
from ingest.replayer.cli import merged
from ingest.replayer.record import build_message
from ingest.replayer.source import read_layout
from ingest.replayer.tests.test_record import DMA_HEADER, ROW, write_csv

T0 = 1_761_609_601_000  # 2025-10-28 00:00:01 UTC, a whole second like DMA rows
MIN = 60_000
SHIPPED = Path(__file__).resolve().parents[1] / "anomalies.yaml"


def doc(*vessels: dict[str, Any]) -> dict[str, Any]:
    return {"version": 1, "mmsi_range": [200000000, 200000999], "report_every_s": 10, "noise_m": 0,
            "vessels": list(vessels)}


def vessel(**kw: Any) -> dict[str, Any]:
    v: dict[str, Any] = {"name": "T", "mmsi": 200000001, "start": [56.0, 11.0], "sog_kn": 12, "cog_deg": 90,
                         "start_min": 0, "event": {"kind": "dark", "at_min": 5, "for_min": 10}, "expect": "DARK_GAP"}
    v.update(kw)
    return v


def layout(tmp_path: Path) -> tuple[int, dict[str, int], str]:
    lay = read_layout(write_csv(tmp_path, ROW))
    return len(lay.columns), lay.field_index, lay.timestamp_format


def test_shipped_file_parses() -> None:
    spec = inject.load(SHIPPED)
    assert len(spec.vessels) == 10 and spec.required_min == 54.0
    assert {v.expect for v in spec.vessels} == {"DARK_GAP", "TELEPORT", None}


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ({"mmsi": 219000000}, "outside mmsi_range"),
        ({"expect": "MMSI_CLASH"}, "expect"),
        ({"event": {"kind": "spoof", "at_min": 5}}, "event.kind"),
        ({"start_min": 4}, "at least 2.0 min"),
        ({"name": "A, B"}, "commas"),
    ],
)
def test_bad_spec(change: dict[str, Any], match: str) -> None:
    with pytest.raises(inject.InjectError, match=match):
        inject.parse(doc(vessel(**change)))


def test_duplicate_mmsi() -> None:
    with pytest.raises(inject.InjectError, match="own mmsi"):
        inject.parse(doc(vessel(), vessel(name="U")))


def test_run_too_short_names_the_fix(tmp_path: Path) -> None:
    spec = inject.parse(doc(vessel()))
    with pytest.raises(inject.InjectError, match=r"cover 10.0 min .* need 16.0 min.*--limit"):
        inject.plan(spec, T0, T0 + 10 * MIN, *layout(tmp_path))


def test_dark_vessel_is_silent_and_labelled(tmp_path: Path) -> None:
    spec = inject.parse(doc(vessel()))
    p = inject.plan(spec, T0, T0 + 20 * MIN, *layout(tmp_path))
    times = [ts for ts, _ in p.rows]
    assert times == sorted(times)
    assert not [t for t in times if T0 + 5 * MIN < t <= T0 + 15 * MIN]
    assert times[-1] <= T0 + 20 * MIN
    (label,) = p.labels
    assert label["trigger_ts"] == T0 + 5 * MIN == max(t for t in times if t <= T0 + 15 * MIN)
    assert label["returned_ts"] == T0 + 15 * MIN + 10_000
    assert (label["mmsi"], label["expect"], label["kind"]) == (200000001, "DARK_GAP", "dark")


def test_jump_shifts_every_later_report(tmp_path: Path) -> None:
    spec = inject.parse(doc(vessel(event={"kind": "jump", "at_min": 5, "km": 30, "bearing_deg": 0},
                                   expect="TELEPORT", sog_kn=0)))
    cols, idx, fmt = layout(tmp_path)
    p = inject.plan(spec, T0, T0 + 10 * MIN, cols, idx, fmt)
    lats = {ts: float(line.split(",")[idx["lat"]]) for ts, line in p.rows}
    assert lats[T0 + 5 * MIN - 10_000] == pytest.approx(56.0)
    assert lats[T0 + 5 * MIN] == lats[T0 + 10 * MIN] == pytest.approx(56.0 + 30_000 / 111_195, abs=1e-4)
    assert p.labels[0]["trigger_ts"] == T0 + 5 * MIN


def test_rows_go_through_build_message_like_csv_rows(tmp_path: Path) -> None:
    spec = inject.parse(doc(vessel()))
    lay = read_layout(write_csv(tmp_path, ROW))
    ts, line = inject.plan(spec, T0, T0 + 20 * MIN, len(lay.columns), lay.field_index, lay.timestamp_format).rows[3]
    assert next(csv.reader(io.StringIO(line))).__len__() == len(DMA_HEADER.split(","))
    key, value = build_message(line, ts, lay.field_index, "run")
    assert key == b"200000001"
    assert b'"name":"T"' in value and b'"sog":12.0' in value


def test_merge_keeps_event_time_order() -> None:
    real = iter([("a", 1), ("b", 3), ("c", 3), ("nots", None)])
    out = list(merged(real, [(0, "x"), (3, "y"), (5, "z")]))
    assert [(line, inj) for line, _, inj in out] == [
        ("x", True), ("a", False), ("b", False), ("c", False), ("y", True), ("z", True), ("nots", False)
    ]


def test_same_input_same_rows(tmp_path: Path) -> None:
    spec = inject.parse({**doc(vessel()), "noise_m": 5})
    a = inject.plan(spec, T0, T0 + 20 * MIN, *layout(tmp_path))
    b = inject.plan(spec, T0, T0 + 20 * MIN, *layout(tmp_path))
    assert a == b
