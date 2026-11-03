"""DARK_GAP and TELEPORT on hand-built tracks.

Geometry used throughout: at 56N one degree of longitude is about 62.2 km, and
10 kn is 5.144 m/s, so 10 kn east for 60 s is 309 m, or 0.00497 degrees.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest

from stream.faust_app.config import DetectionConfig
from stream.faust_app.detect import ALERT_ID_NAMESPACE, AlertType, Watermarks, alert_id
from stream.faust_app.kalman import KalmanConfig
from stream.faust_app.tests.test_tracker import KEY, MMSI, T0, rec
from stream.faust_app.tracker import (
    RawRecord,
    Result,
    Skip,
    VesselState,
    apply,
    decode,
    process_batch,
    scan_dark_gaps,
)

CFG = KalmanConfig()
DCFG = DetectionConfig()
MIN = 60_000
DEG_PER_MIN_10KN = 0.00497  # longitude at 56N
OTHER = b"257000000"
RUN = "20261102T132701Z-6cdc0c"


def other(offset: int, ts: int, partition: int = 3, **kw: Any) -> RawRecord:
    """A second vessel, moored, whose reports move the partition watermark."""
    return rec(offset, key=OTHER, partition=partition, mmsi=257000000, event_ts=ts, lat=57.0, lon=11.0, sog=0.0, **kw)


def moving(offset: int, minute: float, **kw: Any) -> RawRecord:
    """MMSI on a straight 10 kn eastbound track from (56, 10) at T0."""
    return rec(offset, event_ts=T0 + round(minute * MIN), lon=10.0 + DEG_PER_MIN_10KN * minute, **kw)


class Pipeline:
    """What app.handle() does, minus Kafka: batch, write states, advance the
    watermarks, and scan the partitions that are due."""

    def __init__(self, dcfg: DetectionConfig = DCFG) -> None:
        self.dcfg = dcfg
        self.data: dict[bytes, bytes] = {}
        self.watermarks = Watermarks(dcfg)

    def feed(self, *records: RawRecord, scan: bool = True) -> list[dict[str, Any]]:
        recs = list(records)

        def load(i: int) -> bytes | None:
            key = recs[i].key
            assert key is not None
            return self.data.get(key)

        result = process_batch(recs, load, CFG, self.dcfg)
        for _, key, blob in result.writes:
            self.data[key] = blob
        alerts = [v for _, v in result.alerts]
        due = self.watermarks.advance(result.seen.items())
        if scan and due:
            found = scan_dark_gaps(list(self.data.items()), self.watermarks, due, CFG, self.dcfg)
            for _, key, blob in found.writes:
                self.data[key] = blob
            alerts += [v for _, v in found.alerts]
        return [json.loads(a) for a in alerts]

    def state(self, key: bytes = KEY) -> VesselState:
        return VesselState.from_bytes(self.data[key])


# --- DARK_GAP ---------------------------------------------------------------------


def test_dark_gap_from_scan_once_per_gap_with_prediction() -> None:
    p = Pipeline()
    assert p.feed(moving(10, 0), moving(11, 1), other(12, T0 + MIN)) == []
    assert p.feed(other(13, T0 + 30 * MIN)) == []  # 29 min of silence: not yet
    alerts = p.feed(other(14, T0 + 31 * MIN + 1))

    assert len(alerts) == 1
    a = alerts[0]
    assert (a["type"], a["mmsi"], a["run_id"]) == ("DARK_GAP", MMSI, RUN)
    assert a["ts"] == T0 + 31 * MIN  # last seen + 30 min, not the scan time
    assert a["details"]["last_seen_ts"] == T0 + MIN
    assert a["lon"] == pytest.approx(10.0 + DEG_PER_MIN_10KN, abs=1e-4)
    # 30 more minutes at 10 kn east from the last fix.
    assert a["pred_lat"] == pytest.approx(56.0, abs=1e-3)
    assert a["pred_lon"] == pytest.approx(10.0 + DEG_PER_MIN_10KN * 31, abs=2e-3)
    assert a["details"]["pred_sigma_m"] > CFG.meas_sigma_m

    # The silence goes on: later scans do not raise it again.
    assert p.feed(other(15, T0 + 45 * MIN)) == []
    assert p.feed(other(16, T0 + 90 * MIN)) == []
    # Nor does the vessel's return.
    assert p.feed(moving(17, 95)) == []
    assert not p.state().gap_alerted

    # A second gap is a new alert, with a new id.
    again = p.feed(other(18, T0 + 130 * MIN))
    assert [x["type"] for x in again] == ["DARK_GAP"]
    assert again[0]["alert_id"] != a["alert_id"]


def test_dark_gap_on_return_when_no_scan_saw_it() -> None:
    """The scan runs once per DARK_SCAN_STEP_S, so a gap can end between scans. The
    returning fix raises it, with the same bytes the scan would have produced."""
    returned = Pipeline()
    returned.feed(moving(10, 0), moving(11, 1))
    on_return = returned.feed(moving(12, 40))

    scanned = Pipeline()
    scanned.feed(moving(10, 0), moving(11, 1))
    by_scan = scanned.feed(other(12, T0 + 40 * MIN))

    assert [a["type"] for a in on_return] == ["DARK_GAP"]
    assert on_return == by_scan
    assert returned.state().fix_seq == 3  # and the fix itself is tracked normally


@pytest.mark.parametrize(
    ("records", "silence_min"),
    [
        ([moving(10, 0), moving(11, 1)], 30),  # exactly 30 min is not "more than"
        ([rec(10, sog=1.5), rec(11, event_ts=T0 + MIN, sog=1.5, lon=10.00075)], 60),  # 1.5 kn
        ([rec(10, sog=0.0)], 120),  # moored
    ],
)
def test_no_dark_gap(records: list[RawRecord], silence_min: int) -> None:
    p = Pipeline()
    p.feed(*records)
    last = p.state().heard_ts
    assert p.feed(other(20, last + silence_min * MIN)) == []


def test_dark_gap_needs_the_zone_predicate() -> None:
    state, _, _ = run_apply(moving(10, 0), moving(11, 1))
    wm = Watermarks(DCFG)
    wm.advance([((3, RUN), T0 + 60 * MIN)])
    items = [(KEY, state.to_bytes())]
    assert scan_dark_gaps(items, wm, {3}, CFG, zone=lambda m, lat, lon: False).alerts == []
    assert len(scan_dark_gaps(items, wm, {3}, CFG, zone=lambda m, lat, lon: True).alerts) == 1


def test_scan_only_touches_given_partitions() -> None:
    """The app passes only partitions it actively owns and has events for."""
    state, _, _ = run_apply(moving(10, 0), moving(11, 1))
    wm = Watermarks(DCFG)
    wm.advance([((3, RUN), T0 + 60 * MIN)])
    items = [(KEY, state.to_bytes())]
    assert scan_dark_gaps(items, wm, {0, 1, 2, 4, 5}, CFG).scanned == 0
    assert scan_dark_gaps(items, wm, {3}, CFG).scanned == 1


def test_watermark_is_per_run() -> None:
    """Replaying the same day again restarts event time. The new run's vessels must
    not look dark against the old run's watermark."""
    p = Pipeline()
    p.feed(other(10, T0 + 24 * 60 * MIN))  # first run reached the end of the day
    assert p.feed(moving(11, 0, run_id="run-2"), moving(12, 1, run_id="run-2")) == []
    # Scan explicitly: the pipeline would only scan when a step is due.
    found = scan_dark_gaps(list(p.data.items()), p.watermarks, {3}, CFG)
    assert found.scanned == 2
    assert found.alerts == []


def test_watermarks_due_once_per_step() -> None:
    wm = Watermarks(DetectionConfig(dark_scan_step_s=60))
    assert wm.advance([((0, "r"), 1_000)]) == {0}  # first sight
    assert wm.advance([((0, "r"), 59_000)]) == set()
    assert wm.advance([((0, "r"), 61_000), ((1, "r"), 5)]) == {0, 1}
    assert wm.advance([((0, "r"), 10_000)]) == set()  # never goes back
    assert wm.get((0, "r")) == 61_000


# --- TELEPORT ---------------------------------------------------------------------


def run_apply(*records: RawRecord, dcfg: DetectionConfig = DCFG) -> tuple[VesselState, list[Result], list[dict[str, Any]]]:
    state: VesselState | None = None
    results: list[Result] = []
    alerts: list[dict[str, Any]] = []
    for r in records:
        fix = decode(r.key, r.value)
        assert not isinstance(fix, Skip)
        out = apply(state, fix, r.partition, r.offset, CFG, dcfg)
        results.append(out.result)
        state = out.state or state
        alerts += [json.loads(a) for _, a in out.alerts]
    assert state is not None
    return state, results, alerts


def test_teleport_by_implied_speed_is_rejected_from_the_filter() -> None:
    before, _, _ = run_apply(moving(10, 0), moving(11, 1))
    # 5 km north in 60 s: about 162 kn.
    state, results, alerts = run_apply(moving(10, 0), moving(11, 1), moving(12, 2, lat=56.045))
    assert results[-1] == Result.REJECTED
    assert len(alerts) == 1
    a = alerts[0]
    assert (a["type"], a["ts"], a["lat"]) == ("TELEPORT", T0 + 2 * MIN, 56.045)
    assert a["details"]["rules"] == ["implied_speed", "innovation"]
    assert a["details"]["implied_speed_kn"] == pytest.approx(162, rel=0.02)
    # Predicted: one more minute along the track, not where the outlier is.
    assert (a["pred_lat"], a["pred_lon"]) == pytest.approx((56.0, 10.0 + 2 * DEG_PER_MIN_10KN), abs=1e-4)
    # The filter never saw it.
    assert state.estimate.x.tolist() == before.estimate.x.tolist()
    assert (state.fix_seq, state.rejects, state.heard_lat) == (2, 1, 56.045)

    # The next good fix is accepted and lines up with the track.
    _, results, alerts = run_apply(moving(10, 0), moving(11, 1), moving(12, 2, lat=56.045), moving(13, 3))
    assert results[-1] == Result.UPDATED and len(alerts) == 1


def test_teleport_by_innovation_inside_the_noise_window() -> None:
    """3 km in 5 s skips the implied-speed rule (dt < 10 s), but the filter is
    confident after a short dt, so the innovation rule catches it."""
    _, results, alerts = run_apply(moving(10, 0), moving(11, 1), rec(12, event_ts=T0 + MIN + 5_000, lat=56.027))
    assert results[-1] == Result.REJECTED
    assert alerts[0]["details"]["rules"] == ["innovation"]


@pytest.mark.parametrize(
    "noise",
    [
        rec(12, event_ts=T0 + MIN + 5_000, lon=10.00497 + 0.008),  # 500 m in 5 s
        rec(12, event_ts=T0 + MIN + 20_000, lat=56.008),  # 900 m in 20 s: 87 kn, but < 1 km
    ],
)
def test_gps_noise_is_not_a_teleport(noise: RawRecord) -> None:
    _, results, alerts = run_apply(moving(10, 0), moving(11, 1), noise)
    assert results[-1] == Result.UPDATED
    assert alerts == []


def test_rejections_in_a_row_reinitialise_the_filter() -> None:
    jump = dict(lat=56.5)  # 55 km north
    state, results, alerts = run_apply(
        moving(10, 0), moving(11, 1), moving(12, 2, **jump), moving(13, 3, **jump), moving(14, 4, **jump)
    )
    assert results[2:] == [Result.REJECTED, Result.REJECTED, Result.REINITIALISED]
    assert [a["type"] for a in alerts] == ["TELEPORT"]  # one per jump
    assert (state.anchor_lat, state.fix_seq, state.rejects) == (56.5, 3, 0)


def test_repeated_outlier_is_a_duplicate_not_another_rejection() -> None:
    """About 40% of DMA rows are the same report from another base station. Counting
    them would reinitialise the filter on a single outlier."""
    jump = dict(lat=56.5)
    state, results, _ = run_apply(moving(10, 0), moving(11, 1), moving(12, 2, **jump), moving(13, 2, **jump))
    assert results[2:] == [Result.REJECTED, Result.DUPLICATE]
    assert state.rejects == 1


def test_rejected_fix_ends_a_silence() -> None:
    """A vessel that transmits is not dark, even if what it sends is implausible."""
    p = Pipeline()
    p.feed(moving(10, 0), moving(11, 1))
    p.feed(moving(12, 20, lat=56.5))  # rejected
    assert p.feed(other(13, T0 + 45 * MIN)) == []  # 25 min since it was heard


# --- alert identity ---------------------------------------------------------------


def test_same_input_gives_same_alert_id_and_bytes() -> None:
    records = [moving(10, 0), moving(11, 1), moving(12, 2, lat=56.045), moving(13, 3), moving(14, 50)]

    def alerts() -> list[bytes]:
        result = process_batch(records, lambda i: None, CFG)
        return [v for _, v in result.alerts]

    first, second = alerts(), alerts()
    assert len(first) == 2  # TELEPORT, then DARK_GAP when it returns
    assert first == second

    ids = [json.loads(a)["alert_id"] for a in first]
    assert ids[0] == str(uuid.uuid5(ALERT_ID_NAMESPACE, f"TELEPORT|{MMSI}|{RUN}|{T0 + 2 * MIN}"))
    assert ids[1] == alert_id(AlertType.DARK_GAP, MMSI, RUN, T0 + 3 * MIN)

    # The same events in another run are other alerts.
    other_run = process_batch([rec(r.offset, **_fields(r), run_id="run-2") for r in records], lambda i: None, CFG)
    assert {json.loads(v)["alert_id"] for _, v in other_run.alerts}.isdisjoint(ids)


def _fields(r: RawRecord) -> dict[str, Any]:
    assert r.value is not None
    d: dict[str, Any] = json.loads(r.value)
    return {k: d[k] for k in ("event_ts", "lat", "lon")}


def test_redelivered_batch_after_crash_gives_identical_alerts() -> None:
    """ADR 0004's crash case: alerts sent, state not written. Reprocessing from the
    old state sends the same bytes, which the sink dedups on alert_id."""
    old = Pipeline()
    old.feed(moving(10, 0), moving(11, 1))
    snapshot = dict(old.data)

    batch = [moving(12, 2, lat=56.045), moving(13, 50)]
    first = old.feed(*batch)
    crashed = Pipeline()
    crashed.data = snapshot
    assert crashed.feed(*batch) == first
    assert old.feed(*batch) == []  # state was written: everything is redelivered


# --- state ------------------------------------------------------------------------


def test_v1_state_upgrades() -> None:
    v1 = {
        "v": 1,
        "run_id": RUN,
        "anchor": [56.0, 10.0],
        "x": [0.0, 0.0, 5.0, 0.0],
        "P": [float(i == j) for i in range(4) for j in range(4)],
        "last_event_ts": T0,
        "last_fix": [56.0, 10.0],
        "last_offset": 10,
        "fix_seq": 1,
    }
    state = VesselState.from_bytes(json.dumps(v1).encode())
    assert (state.partition, state.heard_ts, state.rejects, state.gap_alerted) == (None, T0, 0, False)
    assert VesselState.from_bytes(state.to_bytes()).to_bytes() == state.to_bytes()


def test_detection_config_validates() -> None:
    with pytest.raises(ValueError):
        DetectionConfig(teleport_max_rejects=1)
    with pytest.raises(ValueError):
        DetectionConfig(dark_gap_s=0)
