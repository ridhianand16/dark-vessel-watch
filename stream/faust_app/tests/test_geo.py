from __future__ import annotations

import math

import pytest

from stream.faust_app.geo import EARTH_RADIUS_M, TangentPlane


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


@pytest.mark.parametrize(
    ("lat0", "lon0", "lat", "lon"),
    [
        (56.0, 10.0, 56.5, 11.2),
        (56.0, 10.0, 55.1, 8.3),
        (-33.9, 151.2, -34.0, 151.3),
        (0.0, 179.9, 0.1, -179.9),  # across the antimeridian
        (56.0, 10.0, 56.0, 10.0),
    ],
)
def test_round_trip(lat0: float, lon0: float, lat: float, lon: float) -> None:
    plane = TangentPlane(lat0, lon0)
    back = plane.to_latlon(*plane.to_xy(lat, lon))
    assert back == pytest.approx((lat, lon), abs=1e-9)


def test_axes_point_east_and_north() -> None:
    plane = TangentPlane(56.0, 10.0)
    x, y = plane.to_xy(56.0, 10.01)
    assert x > 0 and abs(y) < 1.0
    x, y = plane.to_xy(56.01, 10.0)
    assert y > 0 and x == 0.0


@pytest.mark.parametrize("km", [1, 50, 300])
def test_plane_distance_matches_great_circle(km: int) -> None:
    plane = TangentPlane(56.0, 10.0)
    lat, lon = 56.0 + km / 111.2 * 0.6, 10.0 + km / 62.2 * 0.8  # roughly NE, about km away
    d_plane = math.hypot(*plane.to_xy(lat, lon))
    d_true = haversine_m(56.0, 10.0, lat, lon)
    assert d_plane == pytest.approx(d_true, rel=0.002)  # cos(300 km / R) = 0.9989
