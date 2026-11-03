from __future__ import annotations

import pytest

from stream.faust_app.mmsi import StationClass, is_ship, station_class


@pytest.mark.parametrize(
    ("mmsi", "expected"),
    [
        (219026147, StationClass.SHIP),  # Denmark
        (257000000, StationClass.SHIP),  # Norway
        (200000000, StationClass.SHIP),  # lowest first digit 2
        (799999999, StationClass.SHIP),  # highest first digit 7
        (300000000, StationClass.SHIP),
        (412345678, StationClass.SHIP),
        (538001234, StationClass.SHIP),
        (636012345, StationClass.SHIP),
        (982190001, StationClass.CRAFT_WITH_PARENT),
        (2190047, StationClass.COAST),  # 002190047: leading zeros
        (2190000, StationClass.COAST),
        (0, StationClass.COAST),
        (21900000, StationClass.GROUP),  # 021900000
        (99999999, StationClass.GROUP),  # 099999999
        (111219500, StationClass.SAR_AIRCRAFT),
        (100000000, StationClass.OTHER),  # 1 but not 111
        (110999999, StationClass.OTHER),
        (112000000, StationClass.OTHER),
        (812345678, StationClass.HANDHELD),
        (992191234, StationClass.AID_TO_NAVIGATION),
        (999000001, StationClass.AID_TO_NAVIGATION),
        (970123456, StationClass.SART),
        (972123456, StationClass.MOB),
        (974123456, StationClass.EPIRB),
        (971000000, StationClass.OTHER),
        (973000000, StationClass.OTHER),
        (975000000, StationClass.OTHER),
        (900000000, StationClass.OTHER),
        (1_000_000_000, StationClass.OTHER),  # ten digits
        (-1, StationClass.OTHER),
    ],
)
def test_station_class(mmsi: int, expected: StationClass) -> None:
    assert station_class(mmsi) == expected


@pytest.mark.parametrize(
    ("mmsi", "ship"),
    [
        (199999999, False),
        (200000000, True),
        (799999999, True),
        (800000000, False),
        (979999999, False),
        (980000000, True),
        (989999999, True),
        (990000000, False),
    ],
)
def test_is_ship_boundaries(mmsi: int, ship: bool) -> None:
    assert is_ship(mmsi) is ship
