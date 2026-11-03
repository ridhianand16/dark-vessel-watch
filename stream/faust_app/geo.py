"""Local tangent plane: lat/lon in degrees <-> east/north metres around an anchor point.

Pure: no I/O, no Faust.

This is the orthographic projection on a sphere: a point is dropped straight onto the
plane that touches the Earth at the anchor. x points east, y points north. The inverse
is exact, so lat/lon -> (x, y) -> lat/lon gives back the same point.

Distances along the plane shrink by cos(d / R) at distance d from the anchor: 0.01%
at 100 km and 1% at 900 km, which is about the farthest a fast vessel gets in a day.
Using a sphere instead of the WGS84 ellipsoid adds up to about 0.3% scale error. Both
are well below AIS position noise for the speeds and distances involved here.
Points more than 90 degrees of arc from the anchor do not map correctly; a vessel's
track resets long before that (state is per replay run).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

EARTH_RADIUS_M = 6_371_008.8  # mean radius (IUGG)


@dataclass(frozen=True)
class TangentPlane:
    lat0: float
    lon0: float

    def to_xy(self, lat: float, lon: float) -> tuple[float, float]:
        """Degrees -> (east, north) metres from the anchor."""
        phi, phi0 = math.radians(lat), math.radians(self.lat0)
        dlam = math.radians(lon - self.lon0)  # sin/cos below make this wrap-safe
        cos_phi = math.cos(phi)
        x = EARTH_RADIUS_M * cos_phi * math.sin(dlam)
        y = EARTH_RADIUS_M * (math.cos(phi0) * math.sin(phi) - math.sin(phi0) * cos_phi * math.cos(dlam))
        return x, y

    def to_latlon(self, x: float, y: float) -> tuple[float, float]:
        """(east, north) metres from the anchor -> degrees, longitude in [-180, 180)."""
        rho = math.hypot(x, y)
        if rho == 0.0:
            return self.lat0, self.lon0
        c = math.asin(min(rho / EARTH_RADIUS_M, 1.0))
        phi0 = math.radians(self.lat0)
        sin_c, cos_c = math.sin(c), math.cos(c)
        phi = math.asin(cos_c * math.sin(phi0) + y * sin_c * math.cos(phi0) / rho)
        dlam = math.atan2(x * sin_c, rho * math.cos(phi0) * cos_c - y * math.sin(phi0) * sin_c)
        lon = (self.lon0 + math.degrees(dlam) + 180.0) % 360.0 - 180.0
        return math.degrees(phi), lon
