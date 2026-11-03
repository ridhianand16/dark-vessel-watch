from __future__ import annotations

import math

import numpy as np
import pytest

from stream.faust_app.kalman import (
    KNOT_MS,
    Estimate,
    KalmanConfig,
    initial_estimate,
    predict,
    step,
    velocity_from_sog_cog,
)

CFG = KalmanConfig(meas_sigma_m=20.0, accel_psd=0.05)
SPEED_MS = 10 * KNOT_MS  # 10 kn
COURSE = math.radians(45.0)
VX, VY = SPEED_MS * math.sin(COURSE), SPEED_MS * math.cos(COURSE)


def straight_track(n: int, dt_s: float = 10.0, seed: int = 7) -> tuple[Estimate, list[float], list[float]]:
    """Run n noisy fixes of a 10 kn, 045 degree track through the filter.

    The filter starts with zero velocity and the large "unknown" sigma, so it has to
    learn the velocity from positions alone. Returns the final estimate, the
    innovation distances and the NIS values (fixes 2..n).
    """
    rng = np.random.default_rng(seed)
    est = initial_estimate(0.0, 0.0, None, None, CFG)
    dists: list[float] = []
    nis: list[float] = []
    for k in range(1, n):
        t = k * dt_s
        zx, zy = VX * t + rng.normal(0, CFG.meas_sigma_m), VY * t + rng.normal(0, CFG.meas_sigma_m)
        est, innov = step(est, dt_s, zx, zy, CFG)
        dists.append(innov.distance_m)
        nis.append(innov.nis)
    return est, dists, nis


def test_straight_line_converges() -> None:
    n, dt = 200, 10.0
    est, dists, nis = straight_track(n, dt)
    t_end = (n - 1) * dt
    pos_err = math.hypot(est.x[0] - VX * t_end, est.x[1] - VY * t_end)
    vel_err = math.hypot(est.x[2] - VX, est.x[3] - VY)
    pos_sd, vel_sd = math.sqrt(est.P[0, 0]), math.sqrt(est.P[2, 2])
    # It started at zero velocity (5.1 m/s off) and learned it from positions alone.
    # The errors are judged against the filter's own uncertainty: with q = 0.05 it
    # allows for manoeuvres, so it keeps about 1 m/s of velocity sigma at 10 s fixes.
    assert pos_sd < CFG.meas_sigma_m  # better than a single fix
    assert pos_err < 3 * math.sqrt(2) * pos_sd
    assert vel_err < 3 * math.sqrt(2) * vel_sd
    assert vel_sd < 1.5
    # Consistency: innovations are as large as the filter predicts. The NIS mean is 2
    # for a model that fits exactly; a straight track never accelerates, so it sits a
    # little below that (1.4 to 1.8 over 20 seeds).
    assert 1.0 < float(np.mean(nis[50:])) < 2.5


def test_single_outlier_has_large_innovation() -> None:
    est, dists, nis = straight_track(100)
    typical = max(dists[50:])
    t = 100 * 10.0
    outlier_x, outlier_y = VX * t + 2000.0, VY * t  # one fix 2 km off the track
    _, innov = step(est, 10.0, outlier_x, outlier_y, CFG)
    assert innov.distance_m > 1900.0
    assert innov.distance_m > 10 * typical
    assert innov.nis > 1000.0  # chi-square(2) exceeds 13.8 only 0.1% of the time


def test_variable_dt_and_zero_dt() -> None:
    est = initial_estimate(0.0, 0.0, 10.0, 90.0, CFG)  # 10 kn due east
    assert predict(est, 0.0, CFG) is est
    for dt in (2.0, 10.0, 180.0, 0.0, 3.0):
        est, innov = step(est, dt, float(est.x[0] + est.x[2] * dt), float(est.x[1]), CFG)
        assert math.isfinite(innov.nis)
        assert np.allclose(est.P, est.P.T)
        assert np.all(np.linalg.eigvalsh(est.P) > 0)
    with pytest.raises(ValueError):
        predict(est, -1.0, CFG)


def test_uncertainty_grows_with_dt() -> None:
    est = initial_estimate(0.0, 0.0, 10.0, 0.0, CFG)
    short, long_ = predict(est, 10.0, CFG), predict(est, 600.0, CFG)
    assert long_.P[0, 0] > short.P[0, 0] > est.P[0, 0]


def test_initial_velocity_from_sog_cog() -> None:
    est = initial_estimate(0.0, 0.0, 10.0, 90.0, CFG)
    assert est.x[2] == pytest.approx(SPEED_MS)
    assert est.x[3] == pytest.approx(0.0, abs=1e-9)
    assert est.P[2, 2] == pytest.approx(CFG.init_speed_sigma_ms**2)


@pytest.mark.parametrize(
    ("sog", "cog", "expected"),
    [
        (None, 90.0, None),
        (102.3, 90.0, None),  # AIS "not available"
        (10.0, 360.0, None),  # AIS "not available"
        (10.0, None, None),
        (0.0, None, (0.0, 0.0)),  # stationary: course does not matter
        (10.0, 180.0, (0.0, -SPEED_MS)),
    ],
)
def test_velocity_from_sog_cog(
    sog: float | None, cog: float | None, expected: tuple[float, float] | None
) -> None:
    got = velocity_from_sog_cog(sog, cog)
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected, abs=1e-9)


def test_unknown_velocity_uses_wide_sigma() -> None:
    est = initial_estimate(0.0, 0.0, None, None, CFG)
    assert est.x[2] == est.x[3] == 0.0
    assert est.P[2, 2] == pytest.approx(CFG.unknown_speed_sigma_ms**2)


def test_config_rejects_bad_values() -> None:
    with pytest.raises(ValueError):
        KalmanConfig(meas_sigma_m=0.0)
    with pytest.raises(ValueError):
        KalmanConfig(accel_psd=float("nan"))
