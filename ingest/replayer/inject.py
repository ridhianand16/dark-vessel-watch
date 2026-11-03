"""Labelled synthetic vessels mixed into a replay (--inject anomalies.yaml).

Each vessel sails a straight line at constant speed from `start_min` (minutes after the
run's first event_ts) to the end of the run, reporting every `report_every_s`, with
seeded Gaussian position noise. One event changes that:

    dark   no reports in (at_min, at_min + for_min]; the vessel keeps moving and
           reappears on its dead-reckoned track
    jump   from at_min on, every position is shifted km along bearing_deg

Rows are DMA-format CSV lines that go through build_message() like real ones, so
downstream they differ from real traffic only by MMSI. MMSIs must lie in mmsi_range,
a block the real data does not use (the CLI counts real rows in it per run).

Ground truth goes to data/runs/<run_id>/labels.json: per event the vessel, the kind,
the expected alert type (null for a decoy that must not alert) and trigger_ts, the
event time that identifies the alert (ADR 0005): for dark the last report before the
silence, for jump the first shifted report. The scorer derives the expected alert_id
from it. Vessels report until the run's last event_ts, so none of them goes quiet
at the end of the data, which would look like a dark gap that was not injected.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

EARTH_RADIUS_M = 6_371_008.8
KNOT_MS = 1852.0 / 3600.0
EXPECTABLE = ("DARK_GAP", "TELEPORT")
# Every event needs this much track before it (for the filter to settle) and after it
# (dark: the return is seen; jump: the filter re-anchors after three reports).
LEAD_MIN = 2.0
TAIL_MIN = 1.0


class InjectError(Exception):
    pass


@dataclass(frozen=True)
class Event:
    kind: str  # dark | jump
    at_min: float
    for_min: float = 0.0  # dark
    km: float = 0.0  # jump
    bearing_deg: float = 0.0  # jump

    @property
    def end_min(self) -> float:
        return self.at_min + self.for_min


@dataclass(frozen=True)
class Vessel:
    name: str
    mmsi: int
    lat: float
    lon: float
    sog_kn: float
    cog_deg: float
    start_min: float
    event: Event
    expect: str | None


@dataclass(frozen=True)
class Spec:
    mmsi_lo: int
    mmsi_hi: int
    report_every_s: int
    noise_m: float
    vessels: tuple[Vessel, ...]
    sha256: str

    def in_range(self, mmsi: int) -> bool:
        return self.mmsi_lo <= mmsi <= self.mmsi_hi

    @property
    def required_min(self) -> float:
        """Minutes of event time a run must cover for every event to play out."""
        return max(v.event.end_min + TAIL_MIN for v in self.vessels)


def _num(d: dict[str, Any], key: str, where: str, lo: float = -math.inf, hi: float = math.inf) -> float:
    v = d.get(key)
    if isinstance(v, bool) or not isinstance(v, int | float) or not lo <= v <= hi:
        raise InjectError(f"{where}: {key} must be a number in [{lo}, {hi}], got {v!r}")
    return float(v)


def parse(doc: Any, sha256: str = "") -> Spec:
    if not isinstance(doc, dict) or doc.get("version") != 1:
        raise InjectError("expected a mapping with version: 1")
    rng = doc.get("mmsi_range")
    if not (isinstance(rng, list) and len(rng) == 2 and all(type(x) is int for x in rng) and 0 < rng[0] <= rng[1]):
        raise InjectError(f"mmsi_range must be [lo, hi], got {rng!r}")
    every = _num(doc, "report_every_s", "top level", 1, 600)
    spec_vessels: list[Vessel] = []
    raw_vessels = doc.get("vessels")
    if not isinstance(raw_vessels, list) or not raw_vessels:
        raise InjectError("vessels must be a non-empty list")
    for i, v in enumerate(raw_vessels):
        where = f"vessels[{i}]"
        if not isinstance(v, dict):
            raise InjectError(f"{where}: expected a mapping")
        mmsi = v.get("mmsi")
        if type(mmsi) is not int or not rng[0] <= mmsi <= rng[1]:
            raise InjectError(f"{where}: mmsi {mmsi!r} is outside mmsi_range {rng}")
        start = v.get("start")
        if not (isinstance(start, list) and len(start) == 2):
            raise InjectError(f"{where}: start must be [lat, lon]")
        ev = v.get("event")
        if not isinstance(ev, dict) or ev.get("kind") not in ("dark", "jump"):
            raise InjectError(f"{where}: event.kind must be dark or jump")
        at = _num(ev, "at_min", where, 0)
        event = (
            Event("dark", at, for_min=_num(ev, "for_min", where, 0.1))
            if ev["kind"] == "dark"
            else Event("jump", at, km=_num(ev, "km", where, 0.001), bearing_deg=_num(ev, "bearing_deg", where, 0, 360))
        )
        start_min = _num(v, "start_min", where, 0)
        if event.at_min - start_min < LEAD_MIN:
            raise InjectError(f"{where}: event.at_min must be at least {LEAD_MIN} min after start_min")
        expect = v.get("expect")
        if expect is not None and expect not in EXPECTABLE:
            raise InjectError(f"{where}: expect must be one of {EXPECTABLE} or null, got {expect!r}")
        name = v.get("name")
        if not isinstance(name, str) or not name or "," in name or '"' in name:
            raise InjectError(f"{where}: name must be non-empty text without commas or quotes")
        spec_vessels.append(
            Vessel(
                name=name,
                mmsi=mmsi,
                lat=_num({"lat": start[0]}, "lat", where, -80, 80),
                lon=_num({"lon": start[1]}, "lon", where, -180, 180),
                sog_kn=_num(v, "sog_kn", where, 0, 60),
                cog_deg=_num(v, "cog_deg", where, 0, 360),
                start_min=start_min,
                event=event,
                expect=expect,
            )
        )
    mmsis = [v.mmsi for v in spec_vessels]
    if len(set(mmsis)) != len(mmsis):
        raise InjectError("each vessel needs its own mmsi")
    return Spec(rng[0], rng[1], int(every), _num(doc, "noise_m", "top level", 0, 100), tuple(spec_vessels), sha256)


def load(path: Path) -> Spec:
    data = path.read_bytes()
    try:
        doc = yaml.safe_load(data)
    except yaml.YAMLError as e:
        raise InjectError(f"{path}: {e}") from None
    return parse(doc, hashlib.sha256(data).hexdigest())


@dataclass(frozen=True)
class Plan:
    rows: list[tuple[int, str]]
    """(event_ts ms, CSV line), sorted by time then MMSI."""
    labels: list[dict[str, Any]]


def _position(v: Vessel, t_s: float, shifted: bool, noise: tuple[float, float]) -> tuple[float, float]:
    """Flat-earth dead reckoning from the start. Over a run's ~100 km at most the error
    against the tracker's tangent plane is centimetres, far below the noise."""
    speed = v.sog_kn * KNOT_MS
    east = speed * math.sin(math.radians(v.cog_deg)) * t_s + noise[0]
    north = speed * math.cos(math.radians(v.cog_deg)) * t_s + noise[1]
    if shifted:
        east += v.event.km * 1000 * math.sin(math.radians(v.event.bearing_deg))
        north += v.event.km * 1000 * math.cos(math.radians(v.event.bearing_deg))
    lat = v.lat + math.degrees(north / EARTH_RADIUS_M)
    lon = v.lon + math.degrees(east / (EARTH_RADIUS_M * math.cos(math.radians(v.lat))))
    return lat, lon


def csv_line(columns: int, field_index: dict[str, int], values: dict[str, str]) -> str:
    """A CSV line with the layout's columns; fields the layout lacks are left out."""
    cells = [""] * columns
    for f, text in values.items():
        i = field_index.get(f)
        if i is not None:
            cells[i] = text
    return ",".join(cells)


def plan(
    spec: Spec, first_ts: int, last_ts: int, columns: int, field_index: dict[str, int], ts_format: str
) -> Plan:
    """Rows and labels for a run whose CSV rows span [first_ts, last_ts] (epoch ms)."""
    span_min = (last_ts - first_ts) / 60_000
    if span_min < spec.required_min:
        raise InjectError(
            f"the replayed rows cover {span_min:.1f} min of event time; the anomalies need "
            f"{spec.required_min:.1f} min. Replay more rows (raise --limit)."
        )
    step_ms = spec.report_every_s * 1000
    rows: list[tuple[int, int, str]] = []
    labels: list[dict[str, Any]] = []
    for v in spec.vessels:
        rnd = random.Random(v.mmsi)  # same file and run start, same rows
        t0 = first_ts + round(v.start_min * 60) * 1000
        at_ms = first_ts + round(v.event.at_min * 60_000)
        end_ms = first_ts + round(v.event.end_min * 60_000)
        trigger_ts: int | None = None
        returned_ts: int | None = None
        ts = t0
        while ts <= last_ts:
            dark = v.event.kind == "dark" and at_ms < ts <= end_ms
            shifted = v.event.kind == "jump" and ts >= at_ms
            if v.event.kind == "dark" and ts <= at_ms:
                trigger_ts = ts
            if v.event.kind == "dark" and ts > end_ms and returned_ts is None:
                returned_ts = ts
            if shifted and trigger_ts is None:
                trigger_ts = ts
            if not dark:
                noise = (rnd.gauss(0, spec.noise_m), rnd.gauss(0, spec.noise_m))
                lat, lon = _position(v, (ts - t0) / 1000, shifted, noise)
                stamp = dt.datetime.fromtimestamp(ts / 1000, dt.UTC).strftime(ts_format)
                values = {
                    "event_ts": stamp,
                    "mmsi": str(v.mmsi),
                    "lat": f"{lat:.6f}",
                    "lon": f"{lon:.6f}",
                    "sog": f"{v.sog_kn:.1f}",
                    "cog": f"{v.cog_deg:.1f}",
                    "heading": str(round(v.cog_deg) % 360),
                    "nav_status": "Moored" if v.sog_kn == 0 else "Under way using engine",
                    "ship_type": "Cargo",
                    "name": v.name,
                }
                rows.append((ts, v.mmsi, csv_line(columns, field_index, values)))
            ts += step_ms
        assert trigger_ts is not None  # LEAD_MIN and the span check guarantee a report before / at the event
        label: dict[str, Any] = {
            "name": v.name,
            "mmsi": v.mmsi,
            "kind": v.event.kind,
            "expect": v.expect,
            "trigger_ts": trigger_ts,
            "sog_kn": v.sog_kn,
        }
        if v.event.kind == "dark":
            label.update(silent_from_ts=trigger_ts, returned_ts=returned_ts, for_min=v.event.for_min)
        else:
            label.update(km=v.event.km, bearing_deg=v.event.bearing_deg)
        labels.append(label)
    rows.sort()
    return Plan([(ts, line) for ts, _, line in rows], labels)
