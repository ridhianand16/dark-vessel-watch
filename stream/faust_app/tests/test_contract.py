"""mmsi.py agrees with contracts/ais_enriched.json, the rules the Flink job and the tracker
share. The Flink job's StationClassTest loads the same file."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from stream.faust_app.mmsi import SHIP_CLASSES, StationClass, is_ship, station_class

CONTRACT_PATH = Path(__file__).resolve().parents[3] / "contracts" / "ais_enriched.json"
CONTRACT: dict[str, Any] = json.loads(CONTRACT_PATH.read_text())
CLASSES: list[dict[str, Any]] = CONTRACT["station_classes"]


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
