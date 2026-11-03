"""Tracker settings from the environment (then .env), with defaults.

    TRACKER_MEAS_SIGMA_M            20     AIS position noise per axis, metres
    TRACKER_ACCEL_PSD               0.05   process noise q, m^2/s^3
    TRACKER_INIT_SPEED_SIGMA_MS     1.0    first-fix velocity sigma with SOG/COG, m/s
    TRACKER_UNKNOWN_SPEED_SIGMA_MS  10.0   first-fix velocity sigma without, m/s
    TRACKER_BATCH_MAX               1000   records per batch (see app.py)
    TRACKER_BATCH_WITHIN_S          0.1    max wait to fill a batch, seconds
    TRACKER_DATA_DIR                ./data/tracker   RocksDB and Faust local state
    REDPANDA_BROKERS                localhost:19092

Detection thresholds (DetectionConfig; ADR 0005). Every alert rule reads its numbers
from here and nowhere else:

    DARK_GAP_S                      1800   silence before DARK_GAP, event-time seconds
    DARK_GAP_MIN_SOG_KN             2.0    vessel must have been moving faster than this
    DARK_SCAN_STEP_S                60     scan a partition each time its watermark
                                           advances this much event time
    TELEPORT_MAX_SPEED_KN           50.0   implied speed above this is a TELEPORT
    TELEPORT_MIN_DT_S               10     implied speed ignored below this dt (noise)
    TELEPORT_MIN_DISTANCE_M         1000   implied speed and innovation rules ignore
                                           jumps shorter than this (GPS noise)
    TELEPORT_MAX_NIS                50.0   innovation NIS above this is a TELEPORT
    TELEPORT_MAX_REJECTS            3      the Nth rejected fix in a row restarts the filter
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from common.env import setting
from stream.faust_app.kalman import KalmanConfig


@dataclass(frozen=True)
class DetectionConfig:
    dark_gap_s: float = 1800.0
    dark_gap_min_sog_kn: float = 2.0
    dark_scan_step_s: float = 60.0
    teleport_max_speed_kn: float = 50.0
    teleport_min_dt_s: float = 10.0
    teleport_min_distance_m: float = 1000.0
    teleport_max_nis: float = 50.0
    """Chi-square, 2 dof: a fix that fits the model exceeds 13.8 0.1% of the time and
    50 about once in 10^11. Real AIS is heavier-tailed than that, hence the margin."""
    teleport_max_rejects: int = 3

    def __post_init__(self) -> None:
        for name in (
            "dark_gap_s",
            "dark_scan_step_s",
            "teleport_max_speed_kn",
            "teleport_min_distance_m",
            "teleport_max_nis",
        ):
            v = getattr(self, name)
            if not (math.isfinite(v) and v > 0):
                raise ValueError(f"{name} must be a positive finite number, got {v!r}")
        for name in ("dark_gap_min_sog_kn", "teleport_min_dt_s"):
            v = getattr(self, name)
            if not (math.isfinite(v) and v >= 0):
                raise ValueError(f"{name} must be a finite number >= 0, got {v!r}")
        if self.teleport_max_rejects < 2:
            # 1 would restart the filter on every outlier, so nothing is ever rejected.
            raise ValueError(f"teleport_max_rejects must be >= 2, got {self.teleport_max_rejects}")

    @property
    def dark_gap_ms(self) -> int:
        return round(self.dark_gap_s * 1000)

    @property
    def dark_scan_step_ms(self) -> int:
        return max(1, round(self.dark_scan_step_s * 1000))


@dataclass(frozen=True)
class TrackerConfig:
    brokers: str
    data_dir: str
    batch_max: int
    batch_within_s: float
    kalman: KalmanConfig = field(default_factory=KalmanConfig)
    detect: DetectionConfig = field(default_factory=DetectionConfig)


def load() -> TrackerConfig:
    d = KalmanConfig()
    r = DetectionConfig()
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
        detect=DetectionConfig(
            dark_gap_s=float(setting("DARK_GAP_S", str(r.dark_gap_s))),
            dark_gap_min_sog_kn=float(setting("DARK_GAP_MIN_SOG_KN", str(r.dark_gap_min_sog_kn))),
            dark_scan_step_s=float(setting("DARK_SCAN_STEP_S", str(r.dark_scan_step_s))),
            teleport_max_speed_kn=float(setting("TELEPORT_MAX_SPEED_KN", str(r.teleport_max_speed_kn))),
            teleport_min_dt_s=float(setting("TELEPORT_MIN_DT_S", str(r.teleport_min_dt_s))),
            teleport_min_distance_m=float(setting("TELEPORT_MIN_DISTANCE_M", str(r.teleport_min_distance_m))),
            teleport_max_nis=float(setting("TELEPORT_MAX_NIS", str(r.teleport_max_nis))),
            teleport_max_rejects=int(setting("TELEPORT_MAX_REJECTS", str(r.teleport_max_rejects))),
        ),
    )
