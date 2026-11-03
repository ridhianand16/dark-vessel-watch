"""Per-vessel tracking logic. Pure: no I/O, no Faust (app.py is the Faust glue).

The input is ais.enriched (the Flink enrichment job's output: the ais.raw object plus
src_partition, src_offset, station_class and zone tags), or ais.raw directly. Either
way one record goes through three stages:

1. decode()  -> a Fix, or a Skip reason. Skipped records never touch state:
       invalid_key   key is "invalid" (unparseable MMSI) or missing
       decode_error  not UTF-8 JSON object, schema_version != 1, MMSI does not match
                     the key, or no run_id; on ais.enriched also missing or mistyped
                     src_partition, src_offset, station_class or zone tags
       not_a_ship    MMSI is not a ship station (mmsi.py): buoys, base stations, SARTs
       station_class_mismatch  ais.enriched only: the Flink job's station_class is
                     not the class mmsi.py gives the MMSI (contracts/ais_enriched.json)
       no_event_ts   event_ts missing
       no_position   lat/lon missing or out of range (AIS sends 91/181 for "not available")
   On ais.enriched the Flink job has already dropped invalid records (to ais.dlq) and
   duplicates, so the tracker's own checks are a safety net. A record they catch
   anyway is counted as a leak (Leak): leak_invalid for any skip other than
   not_a_ship, leak_station_class, and leak_duplicate for a duplicate result. Non-ship
   stations are valid in ais.enriched and are skipped here as on ais.raw.
2. apply()   -> an Outcome for that vessel, given its stored state:
       started       first fix of this MMSI: new track, anchored at this fix
       restarted     first fix of this MMSI in a new run_id: state reset, as above
       updated       Kalman predict + update
       rejected      TELEPORT (ADR 0005): the fix jumps too far from the track, so it is
                     not fed to the filter. The first of a run of rejections raises
                     a TELEPORT alert
       reinitialised the Nth rejection in a row (teleport_max_rejects): the vessel
                     really is over there, so the filter restarts at this fix
       redelivered   ais.raw offset <= the last one applied to this vessel: the record
                     was already processed before a restart (ADR 0004); nothing changes.
                     On ais.enriched the offset is the record's src_offset, never its
                     own: the Flink job drops records, so enriched offsets run behind
                     raw ones and comparing the two would skip records
       duplicate     same event_ts and position as the last fix heard (the same
                     report heard by several base stations, about 40% of DMA rows);
                     feeding it again would make the filter overconfident, and
                     counting a repeated outlier again would reach the reinit limit.
                     Positions are compared rounded to 5 decimals (about 1 m) the way
                     the Flink job's dedup rounds them (contracts/ais_enriched.json)
       out_of_order  event_ts earlier than the last fix heard in the same run
   started/restarted/updated/reinitialised are "accepted" and produce one ais.tracks
   message. A fix that ends a silence longer than DARK_GAP_S also raises DARK_GAP, if
   the gap scan has not already (one alert per gap: VesselState.gap_alerted). The
   zone condition uses the zone tags of the last accepted fix (detect.near_zone).
3. process_batch() runs both over a batch of records, keeping a working copy of each
   vessel's state, and returns the track and alert messages plus the final state per
   vessel, and the batch's max event_ts per (partition, run_id) for the watermarks.
4. scan_dark_gaps() checks every stored vessel on the given partitions against the
   partition watermark, and raises DARK_GAP for those silent too long.

Output is a pure function of (stored state, record). A redelivered record that was
not yet applied to state produces byte-identical track and alert messages, so a sink
that dedups on (src_partition, src_offset) or alert_id sees an exact copy.

ais.tracks v1 value (JSON, UTF-8), key = str(mmsi) like every topic:
    schema_version   int    1
    mmsi             int
    run_id           str
    event_ts         int    epoch ms UTC, of the fix
    lat, lon         float  filtered position
    vx_ms, vy_ms     float  filtered velocity east/north, m/s
    sog_kn, cog_deg  float  the same velocity as speed and course (cog 0 when stationary)
    pos_sigma_m      float  filtered position uncertainty, sqrt of mean axis variance
    meas_lat, meas_lon  float  the fix as received
    innovation_m     float? distance from predicted to measured position; null on a
                            track's first fix (there is no prediction)
    innovation_nis   float? normalised innovation squared (chi-square, 2 dof); null likewise
    dt_s             float? seconds since the previous accepted fix; null likewise
    fix_seq          int    1 on a track's first fix, then +1 per accepted fix
                            (a reinitialised track keeps counting; innovation and
                            dt_s are null on it, like on a first fix)
    src_partition, src_offset  int  ais.raw coordinates: the message's identity for dedup
                            (on ais.enriched, copied from the record)
    trace_id         str?   passed through from ais.raw
    replay_send_ts   int?   passed through, for end-to-end latency
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable, Collection, Iterable, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any

import numpy as np

from stream.faust_app.config import DetectionConfig
from stream.faust_app.detect import (
    AlertType,
    Watermarks,
    WatermarkKey,
    ZoneTags,
    dark_gap_due,
    encode_alert,
    near_zone,
    teleport_rules,
)
from stream.faust_app.geo import TangentPlane, haversine_m
from stream.faust_app.kalman import (
    KNOT_MS,
    SOG_MAX_VALID_KN,
    Estimate,
    Innovation,
    KalmanConfig,
    initial_estimate,
    predict,
    step,
)
from stream.faust_app.mmsi import is_ship
from stream.faust_app.mmsi import station_class as mmsi_station_class  # decode() has a local station_class

RAW_SCHEMA_VERSION = 1
TRACKS_SCHEMA_VERSION = 1
STATE_VERSION = 3
INVALID_KEY = b"invalid"
DUPLICATE_DECIMALS = 5
_DUPLICATE_SCALE = 10.0**DUPLICATE_DECIMALS


class Skip(StrEnum):
    INVALID_KEY = "invalid_key"
    DECODE_ERROR = "decode_error"
    NOT_A_SHIP = "not_a_ship"
    CLASS_MISMATCH = "station_class_mismatch"
    NO_EVENT_TS = "no_event_ts"
    NO_POSITION = "no_position"


class Result(StrEnum):
    STARTED = "started"
    RESTARTED = "restarted"
    UPDATED = "updated"
    REJECTED = "rejected"
    REINITIALISED = "reinitialised"
    REDELIVERED = "redelivered"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"


ACCEPTED = frozenset({Result.STARTED, Result.RESTARTED, Result.UPDATED, Result.REINITIALISED})
DEFAULT_DETECTION = DetectionConfig()


class Leak(StrEnum):
    """What the tracker's own checks catch on ais.enriched that the Flink job should
    have removed. Each should stay 0; app.py logs them as flink_leaks."""

    INVALID = "leak_invalid"
    STATION_CLASS = "leak_station_class"
    DUPLICATE = "leak_duplicate"


LEAK_SKIPS = frozenset(Skip) - {Skip.NOT_A_SHIP, Skip.CLASS_MISMATCH}


class PartitionMismatch(Exception):
    """An ais.enriched record in another partition than its ais.raw source. ADR 0001
    makes them equal; if they are not, per-vessel order and the gap scan's partition
    ownership no longer hold, so the tracker stops rather than guess."""


@dataclass(frozen=True)
class RawRecord:
    """One consumed record (ais.raw or ais.enriched) and where it was read."""

    key: bytes | None
    value: bytes | None
    partition: int
    offset: int


@dataclass(frozen=True)
class Fix:
    mmsi: int
    run_id: str
    event_ts: int
    lat: float
    lon: float
    sog: float | None
    cog: float | None
    trace_id: str | None
    replay_send_ts: int | None
    src_partition: int | None = None
    src_offset: int | None = None
    """ais.raw coordinates, from an ais.enriched record; None when read from ais.raw."""
    zone: ZoneTags | None = None
    """The Flink job's zone tags; None when read from ais.raw."""


def duplicate_coord(v: float) -> int:
    """A coordinate as it enters the duplicate key: v * 10^5 rounded to the nearest
    integer, ties toward positive infinity, as Java's Math.round does in the Flink job.
    Python's round() sends ties to even and would disagree on exact halves."""
    x = v * _DUPLICATE_SCALE
    f = math.floor(x)
    return f + 1 if x - f >= 0.5 else f  # x - f is exact for |x| < 2^52


def _same_report(fix: Fix, prev: VesselState) -> bool:
    return (
        fix.event_ts == prev.heard_ts
        and duplicate_coord(fix.lat) == duplicate_coord(prev.heard_lat)
        and duplicate_coord(fix.lon) == duplicate_coord(prev.heard_lon)
    )


def _int(v: object) -> int | None:
    return v if type(v) is int else None  # not isinstance: bool is an int


def _float(v: object) -> float | None:
    if type(v) is int or type(v) is float:
        f = float(v)
        return f if math.isfinite(f) else None
    return None


def _zone(msg: dict[str, Any]) -> ZoneTags | None:
    version, eez, mpas, in_port = msg.get("zones_version"), msg.get("eez_id"), msg.get("mpa_ids"), msg.get("in_port")
    if not isinstance(version, str) or not (eez is None or isinstance(eez, str)) or type(in_port) is not bool:
        return None
    if not isinstance(mpas, list) or not all(isinstance(m, str) for m in mpas):
        return None
    d_eez, d_mpa = msg.get("dist_eez_boundary_m"), msg.get("dist_mpa_m")
    if (d_eez is not None and _float(d_eez) is None) or (d_mpa is not None and _float(d_mpa) is None):
        return None
    return ZoneTags(version, eez, tuple(mpas), in_port, _float(d_eez), _float(d_mpa))


def decode(key: bytes | None, value: bytes | None, enriched: bool = False) -> Fix | Skip:
    if key is None or key == INVALID_KEY:
        return Skip.INVALID_KEY
    if value is None:
        return Skip.DECODE_ERROR
    try:
        msg = json.loads(value)
    except ValueError:  # includes UnicodeDecodeError
        return Skip.DECODE_ERROR
    if not isinstance(msg, dict) or _int(msg.get("schema_version")) != RAW_SCHEMA_VERSION:
        return Skip.DECODE_ERROR
    mmsi = _int(msg.get("mmsi"))
    run_id = msg.get("run_id")
    if mmsi is None or str(mmsi).encode() != key or not isinstance(run_id, str) or not run_id:
        return Skip.DECODE_ERROR
    src_partition = src_offset = None
    zone = None
    if enriched:
        src_partition, src_offset = _int(msg.get("src_partition")), _int(msg.get("src_offset"))
        station_class, zone = msg.get("station_class"), _zone(msg)
        if src_partition is None or src_offset is None or not isinstance(station_class, str) or zone is None:
            return Skip.DECODE_ERROR
        # The exact class, not "ship or not": craft_with_parent is a ship too
        # (contracts/ais_enriched.json), and any other disagreement is a contract break.
        if station_class != mmsi_station_class(mmsi).value:
            return Skip.CLASS_MISMATCH
    if not is_ship(mmsi):
        return Skip.NOT_A_SHIP
    event_ts = _int(msg.get("event_ts"))
    if event_ts is None:
        return Skip.NO_EVENT_TS
    lat, lon = _float(msg.get("lat")), _float(msg.get("lon"))
    if lat is None or lon is None or not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return Skip.NO_POSITION
    trace_id = msg.get("trace_id")
    return Fix(
        mmsi=mmsi,
        run_id=run_id,
        event_ts=event_ts,
        lat=lat,
        lon=lon,
        sog=_float(msg.get("sog")),
        cog=_float(msg.get("cog")),
        trace_id=trace_id if isinstance(trace_id, str) else None,
        replay_send_ts=_int(msg.get("replay_send_ts")),
        src_partition=src_partition,
        src_offset=src_offset,
        zone=zone,
    )


@dataclass(frozen=True)
class VesselState:
    """Everything kept per MMSI between records (the Faust table value)."""

    run_id: str
    anchor_lat: float
    anchor_lon: float
    estimate: Estimate
    last_event_ts: int
    """Time of the last accepted fix: the filter's clock."""
    last_lat: float
    last_lon: float
    """The last accepted fix as received; TELEPORT measures jumps from here."""
    last_offset: int
    """Highest ais.raw offset applied to this state, also when the input is ais.enriched
    (its src_offset). All of a vessel's records are in one partition (keyed by MMSI),
    and the Flink job keeps per-key order, so offsets alone order them."""
    fix_seq: int
    partition: int | None
    """ais.raw partition of the vessel's records; picks the watermark for the gap scan.
    None only in states upgraded from v1, until the vessel reports again."""
    last_sog: float | None
    """Reported SOG of the last accepted fix, when valid (DARK_GAP's "last moving")."""
    heard_ts: int
    heard_lat: float
    heard_lon: float
    """The last fix heard, accepted or rejected. DARK_GAP measures silence from heard_ts."""
    trace_id: str | None
    """trace_id of the last fix heard; a DARK_GAP alert carries it."""
    rejects: int
    """TELEPORT rejections in a row since the last accepted fix."""
    gap_alerted: bool
    """DARK_GAP already raised for the current silence; cleared by the next fix heard."""
    zone: ZoneTags | None
    """Zone tags of the last accepted fix (DARK_GAP's zone condition). None when the
    input is ais.raw, and in states from before version 3."""

    def to_bytes(self) -> bytes:
        return json.dumps(
            {
                "v": STATE_VERSION,
                "run_id": self.run_id,
                "anchor": [self.anchor_lat, self.anchor_lon],
                "x": self.estimate.x.tolist(),
                "P": self.estimate.P.ravel().tolist(),
                "last_event_ts": self.last_event_ts,
                "last_fix": [self.last_lat, self.last_lon],
                "last_offset": self.last_offset,
                "fix_seq": self.fix_seq,
                "partition": self.partition,
                "last_sog": self.last_sog,
                "heard": [self.heard_ts, self.heard_lat, self.heard_lon],
                "trace_id": self.trace_id,
                "rejects": self.rejects,
                "gap_alerted": self.gap_alerted,
                "zone": self.zone.to_dict() if self.zone is not None else None,
            },
            separators=(",", ":"),
        ).encode()

    @classmethod
    def from_bytes(cls, blob: bytes) -> VesselState:
        return cls.from_dict(json.loads(blob))

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> VesselState:
        v = d.get("v")
        if v == 1:  # before detections: nothing heard but accepted fixes, no flags
            d.update(
                partition=None,
                last_sog=None,
                heard=[d["last_event_ts"], *d["last_fix"]],
                trace_id=None,
                rejects=0,
                gap_alerted=False,
            )
        if v in (1, 2):  # before zone tags
            d.update(zone=None)
        elif v != STATE_VERSION:
            raise ValueError(f"unsupported vessel state version: {v!r}")
        return cls(
            run_id=d["run_id"],
            anchor_lat=d["anchor"][0],
            anchor_lon=d["anchor"][1],
            estimate=Estimate(
                x=np.array(d["x"], dtype=np.float64),
                P=np.array(d["P"], dtype=np.float64).reshape(4, 4),
            ),
            last_event_ts=d["last_event_ts"],
            last_lat=d["last_fix"][0],
            last_lon=d["last_fix"][1],
            last_offset=d["last_offset"],
            fix_seq=d["fix_seq"],
            partition=d["partition"],
            last_sog=d["last_sog"],
            heard_ts=d["heard"][0],
            heard_lat=d["heard"][1],
            heard_lon=d["heard"][2],
            trace_id=d["trace_id"],
            rejects=d["rejects"],
            gap_alerted=d["gap_alerted"],
            zone=ZoneTags.from_dict(d["zone"]) if d["zone"] is not None else None,
        )

    def speed_kn(self) -> float:
        """Reported SOG of the last accepted fix, else the filter's speed."""
        if self.last_sog is not None:
            return self.last_sog
        return math.hypot(float(self.estimate.x[2]), float(self.estimate.x[3])) / KNOT_MS


@dataclass(frozen=True)
class Outcome:
    result: Result
    state: VesselState | None
    """The vessel's new state; None when it is unchanged."""
    track: bytes | None
    """The ais.tracks value; None unless the fix was accepted."""
    alerts: tuple[tuple[AlertType, bytes], ...] = ()
    """ais.alerts values raised by this fix."""


def _valid_sog(sog: float | None) -> float | None:
    return sog if sog is not None and 0.0 <= sog <= SOG_MAX_VALID_KN else None


def _new_track(fix: Fix, partition: int, offset: int, fix_seq: int, cfg: KalmanConfig) -> VesselState:
    est = initial_estimate(0.0, 0.0, fix.sog, fix.cog, cfg)
    return VesselState(
        run_id=fix.run_id,
        anchor_lat=fix.lat,
        anchor_lon=fix.lon,
        estimate=est,
        last_event_ts=fix.event_ts,
        last_lat=fix.lat,
        last_lon=fix.lon,
        last_offset=offset,
        fix_seq=fix_seq,
        partition=partition,
        last_sog=_valid_sog(fix.sog),
        heard_ts=fix.event_ts,
        heard_lat=fix.lat,
        heard_lon=fix.lon,
        trace_id=fix.trace_id,
        rejects=0,
        gap_alerted=False,
        zone=fix.zone,
    )


def _gap_due(state: VesselState, now_ts: int, dcfg: DetectionConfig) -> bool:
    return dark_gap_due(state.heard_ts, now_ts, state.speed_kn(), state.gap_alerted, near_zone(state.zone, dcfg), dcfg)


def dark_gap_alert(state: VesselState, mmsi: int, cfg: KalmanConfig, dcfg: DetectionConfig) -> bytes:
    """DARK_GAP for the silence after state.heard_ts. Depends only on the state, so the
    scan and the vessel's return produce the same bytes for the same gap.

    details: last_seen_ts (ms), gap_s (the threshold), last_sog_kn (the speed the rule
    used), pred_sigma_m (position uncertainty of the prediction), zone (the zone tags of
    the last accepted fix that made the rule apply, detect.ZoneTags).
    """
    assert state.zone is not None  # near_zone() is false without tags
    ts = state.heard_ts + dcfg.dark_gap_ms
    plane = TangentPlane(state.anchor_lat, state.anchor_lon)
    pred = predict(state.estimate, (ts - state.last_event_ts) / 1000.0, cfg)
    P = pred.P
    return encode_alert(
        AlertType.DARK_GAP,
        mmsi,
        state.run_id,
        trigger_ts=state.heard_ts,
        ts=ts,
        pos=plane.to_latlon(float(state.estimate.x[0]), float(state.estimate.x[1])),
        pred=plane.to_latlon(float(pred.x[0]), float(pred.x[1])),
        details={
            "last_seen_ts": state.heard_ts,
            "gap_s": dcfg.dark_gap_s,
            "last_sog_kn": state.speed_kn(),
            "pred_sigma_m": math.sqrt((float(P[0, 0]) + float(P[1, 1])) / 2.0),
            "zone": state.zone.to_dict(),
        },
        trace_id=state.trace_id,
    )


def apply(
    prev: VesselState | None,
    fix: Fix,
    partition: int,
    offset: int,
    cfg: KalmanConfig,
    dcfg: DetectionConfig = DEFAULT_DETECTION,
) -> Outcome:
    """partition and offset are the fix's ais.raw coordinates."""
    if prev is not None and offset <= prev.last_offset:
        return Outcome(Result.REDELIVERED, None, None)

    if prev is None or prev.run_id != fix.run_id:
        state = _new_track(fix, partition, offset, 1, cfg)
        result = Result.STARTED if prev is None else Result.RESTARTED
        return Outcome(result, state, encode_track(state, fix, partition, offset, None, None))

    if fix.event_ts < prev.heard_ts:
        return Outcome(Result.OUT_OF_ORDER, None, None)
    if _same_report(fix, prev):
        return Outcome(Result.DUPLICATE, None, None)

    alerts: list[tuple[AlertType, bytes]] = []
    # The gap is over. Raise it here if the scan has not: the scan runs only every
    # DARK_SCAN_STEP_S of event time, and not at all for a partition that goes quiet.
    if _gap_due(prev, fix.event_ts, dcfg):
        alerts.append((AlertType.DARK_GAP, dark_gap_alert(prev, fix.mmsi, cfg, dcfg)))

    plane = TangentPlane(prev.anchor_lat, prev.anchor_lon)
    zx, zy = plane.to_xy(fix.lat, fix.lon)
    dt_s = (fix.event_ts - prev.last_event_ts) / 1000.0
    est, innov = step(prev.estimate, dt_s, zx, zy, cfg)
    jump_m = haversine_m(prev.last_lat, prev.last_lon, fix.lat, fix.lon)
    rules = teleport_rules(jump_m, dt_s, innov, dcfg)

    if rules:
        rejects = prev.rejects + 1
        if rejects >= dcfg.teleport_max_rejects:
            state = _new_track(fix, partition, offset, prev.fix_seq + 1, cfg)
            track = encode_track(state, fix, partition, offset, None, None)
            return Outcome(Result.REINITIALISED, state, track, tuple(alerts))
        if prev.rejects == 0:  # one alert per jump, not one per fix after it
            pred = plane.to_latlon(zx - innov.dx, zy - innov.dy)
            details = teleport_details(prev, rules, jump_m, dt_s, innov)
            alerts.append((AlertType.TELEPORT, teleport_alert(fix, pred, details)))
        state = replace(
            prev,
            last_offset=offset,
            partition=partition,
            heard_ts=fix.event_ts,
            heard_lat=fix.lat,
            heard_lon=fix.lon,
            trace_id=fix.trace_id,
            rejects=rejects,
            gap_alerted=False,
        )
        return Outcome(Result.REJECTED, state, None, tuple(alerts))

    state = VesselState(
        run_id=prev.run_id,
        anchor_lat=prev.anchor_lat,
        anchor_lon=prev.anchor_lon,
        estimate=est,
        last_event_ts=fix.event_ts,
        last_lat=fix.lat,
        last_lon=fix.lon,
        last_offset=offset,
        fix_seq=prev.fix_seq + 1,
        partition=partition,
        last_sog=_valid_sog(fix.sog),
        heard_ts=fix.event_ts,
        heard_lat=fix.lat,
        heard_lon=fix.lon,
        trace_id=fix.trace_id,
        rejects=0,
        gap_alerted=False,
        zone=fix.zone,
    )
    track = encode_track(state, fix, partition, offset, innov, dt_s)
    return Outcome(Result.UPDATED, state, track, tuple(alerts))


def teleport_details(
    prev: VesselState, rules: Sequence[str], jump_m: float, dt_s: float, innov: Innovation
) -> dict[str, Any]:
    """TELEPORT details: which rules fired, the jump from the last accepted fix, and
    the innovation against the filter's prediction."""
    return {
        "rules": list(rules),
        "prev_ts": prev.last_event_ts,
        "prev_lat": prev.last_lat,
        "prev_lon": prev.last_lon,
        "dt_s": dt_s,
        "distance_m": jump_m,
        "implied_speed_kn": jump_m / dt_s / KNOT_MS if dt_s > 0 else None,
        "innovation_m": innov.distance_m,
        "innovation_nis": innov.nis,
    }


def teleport_alert(fix: Fix, pred: tuple[float, float], details: dict[str, Any]) -> bytes:
    return encode_alert(
        AlertType.TELEPORT,
        fix.mmsi,
        fix.run_id,
        trigger_ts=fix.event_ts,
        ts=fix.event_ts,
        pos=(fix.lat, fix.lon),
        pred=pred,
        details=details,
        trace_id=fix.trace_id,
    )


def encode_track(
    state: VesselState, fix: Fix, partition: int, offset: int, innov: Innovation | None, dt_s: float | None
) -> bytes:
    x, y, vx, vy = (float(v) for v in state.estimate.x)
    P = state.estimate.P
    lat, lon = TangentPlane(state.anchor_lat, state.anchor_lon).to_latlon(x, y)
    speed = math.hypot(vx, vy)
    msg: dict[str, Any] = {
        "schema_version": TRACKS_SCHEMA_VERSION,
        "mmsi": fix.mmsi,
        "run_id": fix.run_id,
        "event_ts": fix.event_ts,
        "lat": lat,
        "lon": lon,
        "vx_ms": vx,
        "vy_ms": vy,
        "sog_kn": speed / KNOT_MS,
        "cog_deg": math.degrees(math.atan2(vx, vy)) % 360.0 if speed > 0 else 0.0,
        "pos_sigma_m": math.sqrt((float(P[0, 0]) + float(P[1, 1])) / 2.0),
        "meas_lat": fix.lat,
        "meas_lon": fix.lon,
        "innovation_m": innov.distance_m if innov is not None else None,
        "innovation_nis": innov.nis if innov is not None else None,
        "dt_s": dt_s,
        "fix_seq": state.fix_seq,
        "src_partition": partition,
        "src_offset": offset,
        "trace_id": fix.trace_id,
        "replay_send_ts": fix.replay_send_ts,
    }
    return json.dumps(msg, separators=(",", ":")).encode()


@dataclass
class BatchResult:
    tracks: list[tuple[bytes, bytes]] = field(default_factory=list)
    """(key, value) for ais.tracks, in input order."""
    alerts: list[tuple[bytes, bytes]] = field(default_factory=list)
    """(key, value) for ais.alerts, in input order."""
    writes: list[tuple[int, bytes, bytes]] = field(default_factory=list)
    """(index of the vessel's last record in the batch, key, state) per changed vessel."""
    seen: dict[WatermarkKey, int] = field(default_factory=dict)
    """Max event_ts per (partition, run_id) over every decoded fix, for Watermarks."""
    counts: Counter[str] = field(default_factory=Counter)
    """read (records in), one Skip or Result per record, alert_<type> per alert, and
    on ais.enriched a Leak per record the Flink job should have removed. The Skip and
    Result counts add up to read."""


def process_batch(
    records: Sequence[RawRecord],
    load: Callable[[int], bytes | None],
    cfg: KalmanConfig,
    dcfg: DetectionConfig = DEFAULT_DETECTION,
    enriched: bool = False,
) -> BatchResult:
    """Run a batch through decode() and apply(). `enriched`: records are from
    ais.enriched, so the bookmark offset is src_offset and leaks are counted.

    load(i) returns the stored state for records[i].key. It is called at most once per
    key, before any of that key's writes, so it always sees the state from before the
    batch. The caller writes `writes` only after the track and alert messages are
    acknowledged.
    """
    out = BatchResult()
    working: dict[bytes, VesselState | None] = {}
    last_index: dict[bytes, int] = {}
    changed: set[bytes] = set()
    out.counts["read"] += len(records)
    for i, rec in enumerate(records):
        fix = decode(rec.key, rec.value, enriched)
        if isinstance(fix, Skip):
            out.counts[fix.value] += 1
            if enriched:  # decode() returns CLASS_MISMATCH only for enriched records
                if fix in LEAK_SKIPS:
                    out.counts[Leak.INVALID.value] += 1
                elif fix is Skip.CLASS_MISMATCH:
                    out.counts[Leak.STATION_CLASS.value] += 1
            continue
        key = rec.key
        assert key is not None  # decode() returns Skip.INVALID_KEY otherwise
        offset = rec.offset
        if enriched:
            assert fix.src_offset is not None  # decode() returns Skip.DECODE_ERROR otherwise
            if fix.src_partition != rec.partition:
                raise PartitionMismatch(
                    f"ais.enriched/{rec.partition}@{rec.offset} came from ais.raw/{fix.src_partition}"
                )
            offset = fix.src_offset
        wk = (rec.partition, fix.run_id)
        out.seen[wk] = max(fix.event_ts, out.seen.get(wk, fix.event_ts))
        if key not in working:
            blob = load(i)
            working[key] = VesselState.from_bytes(blob) if blob is not None else None
        outcome = apply(working[key], fix, rec.partition, offset, cfg, dcfg)
        out.counts[outcome.result.value] += 1
        if enriched and outcome.result is Result.DUPLICATE:
            out.counts[Leak.DUPLICATE.value] += 1
        if outcome.state is not None:
            working[key] = outcome.state
            last_index[key] = i
            changed.add(key)
        if outcome.track is not None:
            out.tracks.append((key, outcome.track))
        for type_, alert in outcome.alerts:
            out.counts[f"alert_{type_.value.lower()}"] += 1
            out.alerts.append((key, alert))
    for key in changed:
        state = working[key]
        assert state is not None
        out.writes.append((last_index[key], key, state.to_bytes()))
    out.writes.sort()
    return out


@dataclass
class ScanResult:
    alerts: list[tuple[bytes, bytes]] = field(default_factory=list)
    """(key, value) for ais.alerts."""
    writes: list[tuple[int, bytes, bytes]] = field(default_factory=list)
    """(partition, key, state) with gap_alerted set, to write once the alerts are acked."""
    scanned: int = 0


def scan_dark_gaps(
    items: Iterable[tuple[bytes, bytes]],
    watermarks: Watermarks,
    partitions: Collection[int],
    cfg: KalmanConfig,
    dcfg: DetectionConfig = DEFAULT_DETECTION,
) -> ScanResult:
    """DARK_GAP for vessels still silent: watermark - heard_ts > DARK_GAP_S.

    items are the stored (key, state) pairs; vessels outside `partitions` are skipped,
    so the caller decides which partitions are its own (actively assigned, never a
    standby replica). Raises each gap once: the returned states carry gap_alerted.
    """
    out = ScanResult()
    for key, blob in items:
        # Cheap checks on the parsed JSON first: building the numpy arrays for every
        # vessel would make a scan ~10x slower, and nearly all vessels fail these.
        d = json.loads(blob)
        partition = d.get("partition")
        if partition is None or partition not in partitions:
            continue
        out.scanned += 1
        wm = watermarks.get((partition, d["run_id"]))
        if wm is None or d["gap_alerted"] or wm - d["heard"][0] <= dcfg.dark_gap_ms:
            continue
        state = VesselState.from_dict(d)
        if not _gap_due(state, wm, dcfg):
            continue
        out.alerts.append((key, dark_gap_alert(state, int(key), cfg, dcfg)))
        out.writes.append((partition, key, replace(state, gap_alerted=True).to_bytes()))
    return out
