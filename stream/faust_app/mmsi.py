"""Station class of an MMSI, from its leading digits (ITU-R M.585).

An MMSI is nine digits; the leading ones say what kind of station sent it. Only ship
stations get a track: a buoy, a base station or a SART has no Kalman motion model
worth fitting, and its silences are not "going dark".

    first digit 2-7    ship station (MIDxxxxxx, MID = country)          ship
    98                 craft associated with a parent ship (tender, RIB) ship
    00                 coast station (00MIDxxxx)                         not a ship
    0[1-9]             group of ship stations (a call address)           not a ship
    111                SAR aircraft (111MIDxxx)                          not a ship
    8                  handheld VHF with DSC/GNSS                        not a ship
    99                 aid to navigation (buoy, light, virtual AtoN)    not a ship
    970/972/974        AIS-SART / man overboard / EPIRB-AIS              not a ship
    anything else      including MMSIs outside 0-999999999               not a ship

The table is checked longest prefix first, so 00 wins over 0.
"""

from __future__ import annotations

from enum import StrEnum

MMSI_MAX = 999_999_999


class StationClass(StrEnum):
    SHIP = "ship"
    CRAFT_WITH_PARENT = "craft_with_parent"
    COAST = "coast"
    GROUP = "group"
    SAR_AIRCRAFT = "sar_aircraft"
    HANDHELD = "handheld"
    AID_TO_NAVIGATION = "aid_to_navigation"
    SART = "sart"
    MOB = "mob"
    EPIRB = "epirb"
    OTHER = "other"


SHIP_CLASSES = frozenset({StationClass.SHIP, StationClass.CRAFT_WITH_PARENT})

_PREFIXES: tuple[tuple[str, StationClass], ...] = (
    ("970", StationClass.SART),
    ("972", StationClass.MOB),
    ("974", StationClass.EPIRB),
    ("111", StationClass.SAR_AIRCRAFT),
    ("98", StationClass.CRAFT_WITH_PARENT),
    ("99", StationClass.AID_TO_NAVIGATION),
    ("00", StationClass.COAST),
    ("0", StationClass.GROUP),
    ("8", StationClass.HANDHELD),
    *((d, StationClass.SHIP) for d in "234567"),
)


def station_class(mmsi: int) -> StationClass:
    if not 0 <= mmsi <= MMSI_MAX:
        return StationClass.OTHER
    digits = f"{mmsi:09d}"  # an MMSI below 10^8 has leading zeros: 2190261 is 002190261
    for prefix, cls in _PREFIXES:
        if digits.startswith(prefix):
            return cls
    return StationClass.OTHER


def is_ship(mmsi: int) -> bool:
    return station_class(mmsi) in SHIP_CLASSES
