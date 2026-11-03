"""Per-vessel tracking logic. Pure: no I/O, no Faust (app.py is the Faust glue).

One ais.raw record goes through three stages:

1. decode()  -> a Fix, or a Skip reason. Skipped records never touch state:
       invalid_key   key is "invalid" (unparseable MMSI) or missing
       decode_error  not UTF-8 JSON object, schema_version != 1, MMSI does not match
                     the key, or no run_id
       no_event_ts   event_ts missing
       no_position   lat/lon missing or out of range (AIS sends 91/181 for "not available")
2. apply()   -> an Outcome for that vessel, given its stored state:
       started       first fix of this MMSI: new track, anchored at this fix
       restarted     first fix of this MMSI in a new run_id: state reset, as above
       updated       Kalman predict + update
       redelivered   offset <= the last offset applied to this vessel: the record was
                     already processed before a restart (ADR 0004); nothing changes
       duplicate     same event_ts and position as the last accepted fix (the same
                     report heard by several base stations, about 40% of DMA rows);
                     feeding it again would make the filter overconfident
       out_of_order  event_ts earlier than the last accepted fix in the same run
   started/restarted/updated are "accepted" and produce one ais.tracks message.
3. process_batch() runs both over a batch of records, keeping a working copy of each
   vessel's state, and returns the track messages plus the final state per vessel.

Output is a pure function of (stored state, record). A redelivered record that was
not yet applied to state produces byte-identical track messages, so a sink that
dedups on (src_partition, src_offset) sees an exact copy.

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
    src_partition, src_offset  int  ais.raw coordinates: the message's identity for dedup
    trace_id         str?   passed through from ais.raw
    replay_send_ts   int?   passed through, for end-to-end latency
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import numpy as np

from stream.faust_app.geo import TangentPlane
from stream.faust_app.kalman import KNOT_MS, Estimate, Innovation, KalmanConfig, initial_estimate, step

RAW_SCHEMA_VERSION = 1
TRACKS_SCHEMA_VERSION = 1
STATE_VERSION = 1
INVALID_KEY = b"invalid"


class Skip(StrEnum):
    INVALID_KEY = "invalid_key"
    DECODE_ERROR = "decode_error"
    NO_EVENT_TS = "no_event_ts"
    NO_POSITION = "no_position"


class Result(StrEnum):
    STARTED = "started"
    RESTARTED = "restarted"
    UPDATED = "updated"
    REDELIVERED = "redelivered"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"


ACCEPTED = frozenset({Result.STARTED, Result.RESTARTED, Result.UPDATED})


@dataclass(frozen=True)
class RawRecord:
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


def _int(v: object) -> int | None:
    return v if type(v) is int else None  # not isinstance: bool is an int


def _float(v: object) -> float | None:
    if type(v) is int or type(v) is float:
        f = float(v)
        return f if math.isfinite(f) else None
    return None


def decode(key: bytes | None, value: bytes | None) -> Fix | Skip:
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
    )


@dataclass(frozen=True)
class VesselState:
    """Everything kept per MMSI between records (the Faust table value)."""

    run_id: str
    anchor_lat: float
    anchor_lon: float
    estimate: Estimate
    last_event_ts: int
    last_lat: float
    last_lon: float
    last_offset: int
    """Highest ais.raw offset applied to this state. All of a vessel's records are in
    one partition (keyed by MMSI), so offsets alone order them."""
    fix_seq: int

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
            },
            separators=(",", ":"),
        ).encode()

    @classmethod
    def from_bytes(cls, blob: bytes) -> VesselState:
        d = json.loads(blob)
        if d.get("v") != STATE_VERSION:
            raise ValueError(f"unsupported vessel state version: {d.get('v')!r}")
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
        )


@dataclass(frozen=True)
class Outcome:
    result: Result
    state: VesselState | None
    """The vessel's new state; None when it is unchanged."""
    track: bytes | None
    """The ais.tracks value; None unless the fix was accepted."""


def _new_track(fix: Fix, offset: int, cfg: KalmanConfig) -> VesselState:
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
        fix_seq=1,
    )


def apply(prev: VesselState | None, fix: Fix, partition: int, offset: int, cfg: KalmanConfig) -> Outcome:
    if prev is not None and offset <= prev.last_offset:
        return Outcome(Result.REDELIVERED, None, None)

    if prev is None or prev.run_id != fix.run_id:
        state = _new_track(fix, offset, cfg)
        result = Result.STARTED if prev is None else Result.RESTARTED
        return Outcome(result, state, encode_track(state, fix, partition, offset, None, None))

    if fix.event_ts < prev.last_event_ts:
        return Outcome(Result.OUT_OF_ORDER, None, None)
    if fix.event_ts == prev.last_event_ts and fix.lat == prev.last_lat and fix.lon == prev.last_lon:
        return Outcome(Result.DUPLICATE, None, None)

    plane = TangentPlane(prev.anchor_lat, prev.anchor_lon)
    zx, zy = plane.to_xy(fix.lat, fix.lon)
    dt_s = (fix.event_ts - prev.last_event_ts) / 1000.0
    est, innov = step(prev.estimate, dt_s, zx, zy, cfg)
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
    )
    return Outcome(Result.UPDATED, state, encode_track(state, fix, partition, offset, innov, dt_s))


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
    writes: list[tuple[int, bytes, bytes]] = field(default_factory=list)
    """(index of the vessel's last record in the batch, key, state) per changed vessel."""
    counts: Counter[str] = field(default_factory=Counter)


def process_batch(
    records: Sequence[RawRecord], load: Callable[[int], bytes | None], cfg: KalmanConfig
) -> BatchResult:
    """Run a batch through decode() and apply().

    load(i) returns the stored state for records[i].key. It is called at most once per
    key, before any of that key's writes, so it always sees the state from before the
    batch. The caller writes `writes` only after the track messages are acknowledged.
    """
    out = BatchResult()
    working: dict[bytes, VesselState | None] = {}
    last_index: dict[bytes, int] = {}
    changed: set[bytes] = set()
    for i, rec in enumerate(records):
        fix = decode(rec.key, rec.value)
        if isinstance(fix, Skip):
            out.counts[fix.value] += 1
            continue
        key = rec.key
        assert key is not None  # decode() returns Skip.INVALID_KEY otherwise
        if key not in working:
            blob = load(i)
            working[key] = VesselState.from_bytes(blob) if blob is not None else None
        outcome = apply(working[key], fix, rec.partition, rec.offset, cfg)
        out.counts[outcome.result.value] += 1
        if outcome.state is not None:
            working[key] = outcome.state
            last_index[key] = i
            changed.add(key)
        if outcome.track is not None:
            out.tracks.append((key, outcome.track))
    for key in changed:
        state = working[key]
        assert state is not None
        out.writes.append((last_index[key], key, state.to_bytes()))
    out.writes.sort()
    return out
