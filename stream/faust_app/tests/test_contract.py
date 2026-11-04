"""mmsi.py and the tracker's duplicate check agree with contracts/ais_enriched.json, the
rules the Flink job and the tracker share. The Flink job's StationClassTest and DedupTest
load the same file."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from stream.faust_app.kalman import SOG_MAX_VALID_KN
from stream.faust_app.mmsi import SHIP_CLASSES, StationClass, is_ship, station_class
from stream.faust_app.tracker import DUPLICATE_DECIMALS, _valid_sog, duplicate_coord

CONTRACT_PATH = Path(__file__).resolve().parents[3] / "contracts" / "ais_enriched.json"
CONTRACT: dict[str, Any] = json.loads(CONTRACT_PATH.read_text())
CLASSES: list[dict[str, Any]] = CONTRACT["station_classes"]
DUPLICATE_KEY: dict[str, Any] = CONTRACT["duplicate_key"]
SOG: dict[str, Any] = CONTRACT["sog"]


def test_station_classes_are_exactly_the_contract() -> None:
    assert [c["wire"] for c in CLASSES] == [c.value for c in StationClass]


def test_ship_classes_are_exactly_the_contract() -> None:
    assert {c["wire"] for c in CLASSES if c["is_ship"]} == {c.value for c in SHIP_CLASSES}


@pytest.mark.parametrize(
    ("mmsi", "wire", "ship"),
    [(m, c["wire"], c["is_ship"]) for c in CLASSES for m in c["example_mmsis"]],
)
def test_example_mmsis(mmsi: int, wire: str, ship: bool) -> None:
    assert station_class(mmsi).value == wire
    assert is_ship(mmsi) is ship


def test_duplicate_decimals_are_the_contract() -> None:
    assert DUPLICATE_DECIMALS == DUPLICATE_KEY["coordinate_decimals"]


@pytest.mark.parametrize(
    ("coordinate", "rounded"), [(e["coordinate"], e["rounded"]) for e in DUPLICATE_KEY["coordinate_examples"]]
)
def test_duplicate_coordinate_rounding(coordinate: float, rounded: int) -> None:
    assert duplicate_coord(coordinate) == rounded


def test_sog_bounds_are_the_contract() -> None:
    assert SOG_MAX_VALID_KN == SOG["max_valid_kn"]
    lo, hi, na = SOG["min_valid_kn"], SOG["max_valid_kn"], SOG["not_available_kn"]
    assert _valid_sog(lo) == lo and _valid_sog(hi) == hi
    assert _valid_sog(lo - 0.1) is None and _valid_sog(na) is None
