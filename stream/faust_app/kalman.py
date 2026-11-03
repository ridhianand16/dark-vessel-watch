"""Constant-velocity Kalman filter for one vessel. Pure numpy: no I/O, no Faust.

State is [x, y, vx, vy]: metres east/north on the vessel's tangent plane (geo.py) and
m/s. Each AIS fix measures [x, y]. Time steps vary (AIS reports every 2 s to 6 min,
depending on speed and class), so F and Q are built from dt on every step.

Process noise is the continuous white-noise acceleration model: per axis,
Q = q * [[dt^3/3, dt^2/2], [dt^2/2, dt]], where q (accel_psd, m^2/s^3) says how hard
the vessel is assumed to manoeuvre. Measurement noise is isotropic, sigma metres per
axis. Both come from config (config.py), not code.

The covariance update uses the Joseph form, which stays symmetric and positive
definite under rounding. That matters here because a vessel can be updated thousands
of times a day without a reset.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

Array = npt.NDArray[np.float64]

KNOT_MS = 1852.0 / 3600.0

# AIS "not available" values: SOG 102.3 kn, COG 360.0 degrees.
SOG_MAX_VALID_KN = 102.2
COG_MAX_VALID_DEG = 360.0
# Below this, COG is noise (a moored vessel's COG wanders), so the velocity is just zero.
STATIONARY_KN = 0.1

_H = np.array([[1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0]])
_I4 = np.eye(4)


@dataclass(frozen=True)
class KalmanConfig:
    meas_sigma_m: float = 20.0
    """AIS position noise per axis. Includes GNSS error and the 1 s timestamp resolution."""
    accel_psd: float = 0.05
    """Process noise q, m^2/s^3. Velocity variance grows by q * dt between fixes."""
    init_speed_sigma_ms: float = 1.0
    """Velocity sigma per axis on a track's first fix, when SOG/COG are available."""
    unknown_speed_sigma_ms: float = 10.0
    """Velocity sigma per axis on a first fix without usable SOG/COG (about 20 kn)."""

    def __post_init__(self) -> None:
        for name in ("meas_sigma_m", "accel_psd", "init_speed_sigma_ms", "unknown_speed_sigma_ms"):
            v = getattr(self, name)
            if not (math.isfinite(v) and v > 0):
                raise ValueError(f"{name} must be a positive finite number, got {v!r}")


@dataclass(frozen=True)
class Estimate:
    x: Array
    """[x, y, vx, vy], shape (4,)."""
    P: Array
    """Covariance, shape (4, 4)."""


@dataclass(frozen=True)
class Innovation:
    dx: float
    dy: float
    nis: float
    """Normalised innovation squared, y' S^-1 y. Chi-square with 2 degrees of freedom
    when the model fits: mean 2, and above 13.8 only 0.1% of the time."""

    @property
    def distance_m(self) -> float:
        """How far the fix landed from where the filter predicted it."""
        return math.hypot(self.dx, self.dy)


def velocity_from_sog_cog(sog_kn: float | None, cog_deg: float | None) -> tuple[float, float] | None:
    """(vx, vy) in m/s, or None when SOG/COG cannot give a velocity."""
    if sog_kn is None or not 0.0 <= sog_kn <= SOG_MAX_VALID_KN:
        return None
    if sog_kn < STATIONARY_KN:
        return 0.0, 0.0
    if cog_deg is None or not 0.0 <= cog_deg < COG_MAX_VALID_DEG:
        return None
    v = sog_kn * KNOT_MS
    cog = math.radians(cog_deg)  # clockwise from north
    return v * math.sin(cog), v * math.cos(cog)


def initial_estimate(
    x: float, y: float, sog_kn: float | None, cog_deg: float | None, cfg: KalmanConfig
) -> Estimate:
    """A new track at a measured position, with velocity from SOG/COG when they are usable."""
    vel = velocity_from_sog_cog(sog_kn, cog_deg)
    if vel is None:
        vx, vy, sv = 0.0, 0.0, cfg.unknown_speed_sigma_ms
    else:
        (vx, vy), sv = vel, cfg.init_speed_sigma_ms
    sm = cfg.meas_sigma_m
    return Estimate(
        x=np.array([x, y, vx, vy]),
        P=np.diag([sm * sm, sm * sm, sv * sv, sv * sv]),
    )


def predict(est: Estimate, dt_s: float, cfg: KalmanConfig) -> Estimate:
    """Move the estimate dt_s seconds forward. dt_s == 0 returns it unchanged."""
    if dt_s < 0:
        raise ValueError(f"dt_s must be >= 0, got {dt_s}")
    if dt_s == 0:
        return est
    F = np.array([[1.0, 0.0, dt_s, 0.0], [0.0, 1.0, 0.0, dt_s], [0.0, 0.0, 1.0, 0.0], [0.0, 0.0, 0.0, 1.0]])
    q = cfg.accel_psd
    dt2, dt3 = dt_s * dt_s, dt_s * dt_s * dt_s
    pp, pv, vv = q * dt3 / 3.0, q * dt2 / 2.0, q * dt_s
    Q = np.array([[pp, 0.0, pv, 0.0], [0.0, pp, 0.0, pv], [pv, 0.0, vv, 0.0], [0.0, pv, 0.0, vv]])
    return Estimate(x=F @ est.x, P=F @ est.P @ F.T + Q)


def update(prior: Estimate, zx: float, zy: float, cfg: KalmanConfig) -> tuple[Estimate, Innovation]:
    """Correct a predicted estimate with a measured position (metres)."""
    r = cfg.meas_sigma_m * cfg.meas_sigma_m
    R = np.array([[r, 0.0], [0.0, r]])
    y = np.array([zx, zy]) - _H @ prior.x
    PHt = prior.P @ _H.T
    S = _H @ PHt + R
    S_inv = np.linalg.inv(S)  # 2x2, always well conditioned because R > 0
    K = PHt @ S_inv
    IKH = _I4 - K @ _H
    P = IKH @ prior.P @ IKH.T + K @ R @ K.T
    P = (P + P.T) / 2.0
    nis = float(y @ S_inv @ y)
    return Estimate(x=prior.x + K @ y, P=P), Innovation(dx=float(y[0]), dy=float(y[1]), nis=nis)


def step(est: Estimate, dt_s: float, zx: float, zy: float, cfg: KalmanConfig) -> tuple[Estimate, Innovation]:
    """predict() then update(): the innovation is measured against the prediction."""
    return update(predict(est, dt_s, cfg), zx, zy, cfg)
