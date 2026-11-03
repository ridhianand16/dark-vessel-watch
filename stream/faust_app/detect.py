"""Alert rules and the ais.alerts message. Pure: no I/O, no Faust (ADR 0005).

tracker.py calls these while it applies fixes (TELEPORT, and DARK_GAP when a vessel
comes back) and from scan_dark_gaps() (DARK_GAP while a vessel is still silent).
Thresholds come from config.DetectionConfig only.

Every alert is a pure function of the vessel's stored state and, for TELEPORT, the
record that triggered it. There are no wall-clock fields, so reprocessing produces
byte-identical messages, and alert_id is stable across runs of the tracker.

ais.alerts v1 value (JSON, UTF-8), key = str(mmsi):
    schema_version  int    1
    alert_id        str    UUIDv5 of (type, mmsi, run_id, trigger_ts): the dedup key
    type            str    DARK_GAP | TELEPORT
    mmsi            int
    run_id          str
    ts              int    epoch ms UTC, event time at which the rule became true:
                           TELEPORT: the offending fix; DARK_GAP: last_seen + gap
    lat, lon        float  TELEPORT: the fix as received;
                           DARK_GAP: filtered position at the last accepted fix
    pred_lat, pred_lon  float  where the Kalman filter puts the vessel at `ts`
    details         object rule-specific, see teleport_details() / tracker.dark_gap_alert()
    trace_id        str?   of the triggering ais.raw record (DARK_GAP: the last one heard)
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Iterable, Mapping
from enum import StrEnum
from typing import Any

from stream.faust_app.config import DetectionConfig
from stream.faust_app.kalman import KNOT_MS, Innovation

ALERTS_SCHEMA_VERSION = 1

# Fixed forever: changing it changes every alert_id, which breaks dedup against
# alerts already stored.
ALERT_ID_NAMESPACE = uuid.UUID("5b0f7c1e-3d2a-5e8b-9a41-6c7d8e9f0a1b")


class AlertType(StrEnum):
    DARK_GAP = "DARK_GAP"
    TELEPORT = "TELEPORT"


class TeleportRule(StrEnum):
    IMPLIED_SPEED = "implied_speed"
    INNOVATION = "innovation"


ZonePredicate = Callable[[int, float, float], bool]
"""(mmsi, lat, lon) -> whether a gap starting there counts as DARK_GAP. M3 replaces it
with the PostGIS rules: near an EEZ boundary or protected area, not in port, not at
the receiver coverage edge."""


def everywhere(mmsi: int, lat: float, lon: float) -> bool:
    return True


def alert_id(type_: AlertType, mmsi: int, run_id: str, trigger_ts: int) -> str:
    """Same inputs, same id, in any process. trigger_ts identifies the event:
    TELEPORT: the offending fix's event_ts; DARK_GAP: event_ts of the last fix heard
    before the gap, so one gap has one id however late or often it is detected."""
    return str(uuid.uuid5(ALERT_ID_NAMESPACE, f"{type_.value}|{mmsi}|{run_id}|{trigger_ts}"))


def encode_alert(
    type_: AlertType,
    mmsi: int,
    run_id: str,
    trigger_ts: int,
    ts: int,
    pos: tuple[float, float],
    pred: tuple[float, float],
    details: Mapping[str, Any],
    trace_id: str | None,
) -> bytes:
    msg: dict[str, Any] = {
        "schema_version": ALERTS_SCHEMA_VERSION,
        "alert_id": alert_id(type_, mmsi, run_id, trigger_ts),
        "type": type_.value,
        "mmsi": mmsi,
        "run_id": run_id,
        "ts": ts,
        "lat": pos[0],
        "lon": pos[1],
        "pred_lat": pred[0],
        "pred_lon": pred[1],
        "details": dict(details),
        "trace_id": trace_id,
    }
    return json.dumps(msg, separators=(",", ":")).encode()


# --- TELEPORT ---------------------------------------------------------------------


def teleport_rules(distance_m: float, dt_s: float, innov: Innovation, cfg: DetectionConfig) -> list[TeleportRule]:
    """Which TELEPORT rules a fix breaks, given the jump from the last accepted fix.

    Jumps shorter than teleport_min_distance_m never count (GPS noise). Implied speed
    is also skipped below teleport_min_dt_s, where 1 s timestamp resolution makes it
    meaningless; a large jump inside that window is still caught by the innovation
    rule, because the filter's uncertainty is small after a short dt.
    """
    rules: list[TeleportRule] = []
    if distance_m >= cfg.teleport_min_distance_m and dt_s >= cfg.teleport_min_dt_s and dt_s > 0:
        if distance_m / dt_s / KNOT_MS > cfg.teleport_max_speed_kn:
            rules.append(TeleportRule.IMPLIED_SPEED)
    if innov.distance_m >= cfg.teleport_min_distance_m and innov.nis > cfg.teleport_max_nis:
        rules.append(TeleportRule.INNOVATION)
    return rules


# --- DARK_GAP ---------------------------------------------------------------------


def dark_gap_due(
    last_heard_ts: int, now_ts: int, speed_kn: float, alerted: bool, in_zone: bool, cfg: DetectionConfig
) -> bool:
    """The DARK_GAP rule. now_ts is event time: the partition watermark while the
    vessel is silent, or the event_ts of the fix that ends the gap."""
    return (
        not alerted
        and now_ts - last_heard_ts > cfg.dark_gap_ms
        and speed_kn > cfg.dark_gap_min_sog_kn
        and in_zone
    )


WatermarkKey = tuple[int, str]
"""(partition, run_id). Per run because each replay restarts event time: a second
replay of the same day would otherwise sit a whole day behind the first run's
watermark, and every vessel in it would look dark at once."""


class Watermarks:
    """Max event_ts seen per (partition, run_id), and when each is due for a gap scan.

    In memory only. After a restart the first batch re-establishes it; nothing is lost,
    because a gap that ends before the next scan is caught when the vessel reports
    again (tracker.apply), with the same alert_id and bytes.
    """

    def __init__(self, cfg: DetectionConfig) -> None:
        self._step_ms = cfg.dark_scan_step_ms
        self._wm: dict[WatermarkKey, int] = {}
        self._scanned_step: dict[WatermarkKey, int] = {}

    def get(self, key: WatermarkKey) -> int | None:
        return self._wm.get(key)

    def advance(self, seen: Iterable[tuple[WatermarkKey, int]]) -> set[int]:
        """Merge a batch's max event_ts per key. Returns the partitions whose watermark
        entered a new scan step (event time, so the scan rate follows replay speed)."""
        due: set[int] = set()
        for key, ts in seen:
            wm = max(ts, self._wm.get(key, ts))
            self._wm[key] = wm
            step = wm // self._step_ms
            if step > self._scanned_step.get(key, -1):
                self._scanned_step[key] = step
                due.add(key[0])
        return due
