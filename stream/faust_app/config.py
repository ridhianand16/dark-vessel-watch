"""Tracker settings from the environment (then .env), with defaults.

    TRACKER_MEAS_SIGMA_M            20     AIS position noise per axis, metres
    TRACKER_ACCEL_PSD               0.05   process noise q, m^2/s^3
    TRACKER_INIT_SPEED_SIGMA_MS     1.0    first-fix velocity sigma with SOG/COG, m/s
    TRACKER_UNKNOWN_SPEED_SIGMA_MS  10.0   first-fix velocity sigma without, m/s
    TRACKER_BATCH_MAX               1000   records per batch (see app.py)
    TRACKER_BATCH_WITHIN_S          0.1    max wait to fill a batch, seconds
    TRACKER_DATA_DIR                ./data/tracker   RocksDB and Faust local state
    REDPANDA_BROKERS                localhost:19092
"""

from __future__ import annotations

from dataclasses import dataclass, field

from common.env import setting
from stream.faust_app.kalman import KalmanConfig


@dataclass(frozen=True)
class TrackerConfig:
    brokers: str
    data_dir: str
    batch_max: int
    batch_within_s: float
    kalman: KalmanConfig = field(default_factory=KalmanConfig)


def load() -> TrackerConfig:
    d = KalmanConfig()
    return TrackerConfig(
        brokers=setting("REDPANDA_BROKERS", "localhost:19092"),
        data_dir=setting("TRACKER_DATA_DIR", "./data/tracker"),
        batch_max=int(setting("TRACKER_BATCH_MAX", "1000")),
        batch_within_s=float(setting("TRACKER_BATCH_WITHIN_S", "0.1")),
        kalman=KalmanConfig(
            meas_sigma_m=float(setting("TRACKER_MEAS_SIGMA_M", str(d.meas_sigma_m))),
            accel_psd=float(setting("TRACKER_ACCEL_PSD", str(d.accel_psd))),
            init_speed_sigma_ms=float(setting("TRACKER_INIT_SPEED_SIGMA_MS", str(d.init_speed_sigma_ms))),
            unknown_speed_sigma_ms=float(setting("TRACKER_UNKNOWN_SPEED_SIGMA_MS", str(d.unknown_speed_sigma_ms))),
        ),
    )
