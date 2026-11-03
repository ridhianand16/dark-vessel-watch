from __future__ import annotations

import json

from typing import Any

from reconcile.score_alerts import Stored, describe_zone, expected_ids, score
from stream.faust_app.detect import AlertType, alert_id

RUN = "20261103T120000Z-abc123"


def doc() -> dict[str, Any]:
    return {
        "run_id": RUN,
        "status": "complete",
        "mmsi_range": [200000000, 200000999],
        "rows_injected": 100,
        "csv_rows_in_mmsi_range": 0,
        "labels": [
            {"name": "D1", "mmsi": 200000001, "kind": "dark", "expect": "DARK_GAP", "trigger_ts": 1000},
            {"name": "J1", "mmsi": 200000011, "kind": "jump", "expect": "TELEPORT", "trigger_ts": 2000},
            {"name": "M", "mmsi": 200000021, "kind": "dark", "expect": None, "trigger_ts": 3000},
        ],
    }


def stored(type_: str, mmsi: int, ts: int) -> Stored:
    return Stored(alert_id(AlertType(type_), mmsi, RUN, ts), type_, mmsi)


def test_expected_ids_use_the_tracker_formula() -> None:
    assert set(expected_ids(doc())) == {
        alert_id(AlertType.DARK_GAP, 200000001, RUN, 1000),
        alert_id(AlertType.TELEPORT, 200000011, RUN, 2000),
    }


def test_all_found() -> None:
    s = score(doc(), [stored("DARK_GAP", 200000001, 1000), stored("TELEPORT", 200000011, 2000)])
    assert s.ok and s.tp == {"DARK_GAP": 1, "TELEPORT": 1}


def test_wrong_time_is_fp_and_fn_decoy_alert_is_fp_real_is_unscored() -> None:
    s = score(
        doc(),
        [
            stored("DARK_GAP", 200000001, 1000),
            stored("TELEPORT", 200000011, 2010),  # one report late
            stored("DARK_GAP", 200000021, 3000),  # the moored decoy
            stored("DARK_GAP", 219000001, 5000),  # a real vessel
            Stored("invalid:0:7", "", None),  # undecodable record
        ],
    )
    assert not s.ok
    assert s.tp == {"DARK_GAP": 1}
    assert s.fp == {"TELEPORT": 1, "DARK_GAP": 1}
    assert s.fn == {"TELEPORT": 1}
    assert s.real == {"DARK_GAP": 1, "": 1}
    assert len(s.missed) == 1 and len(s.unexpected) == 2


def test_describe_zone() -> None:
    details = json.dumps({"zone": {"zones_version": "v1", "eez_id": "eez:5674", "mpa_ids": ["wdpa:1"],
                                   "in_port": False, "dist_eez_boundary_m": 18520.0, "dist_mpa_m": 0.0}})
    assert describe_zone(details) == (
        "protected area 0.0 nm (inside: wdpa:1), EEZ boundary 10.0 nm, in_port False, zones v1"
    )
    assert describe_zone("{}") == "no zone tags"
