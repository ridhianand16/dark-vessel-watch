"""Sample the running pipeline every few seconds while the report's replay runs.

Each sample holds, at one wall time:

    groups  per consumer group on the run's path, the sum over its partitions of the
            committed offset (how far it got) and of the lag, from `rpk group
            describe` inside the Redpanda container
    memory  per dvw-* container, memory in use and its limit, from `docker stats`
    flink   the running enrichment job's checkpoint counters and recent checkpoint
            history, from the Flink REST API

A source that fails (the JobManager is down, a container is restarting) is left out
of that sample, never fatal: the faults the report injects make some of them fail
on purpose. The parse and summary functions are pure, so they are unit-tested
without a stack.
"""

from __future__ import annotations

import json
import re
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from reconcile.kafka import ALERTS_SINK_GROUP, FLINK_GROUP, TRACKER_GROUP

REDPANDA_CONTAINER = "dvw-redpanda"
CONTAINER_PREFIX = "dvw-"
JOB_NAME = "dvw-enrichment"
# Group -> what it is, in pipeline order. Names: sinks/clickhouse/specs.py,
# sinks/postgis_vessels/sink.py, EnrichmentJob.GROUP_ID, the tracker's app id.
STAGES: dict[str, str] = {
    "dvw-raw-sink": "raw sink, ais.raw to dvw.ais_raw",
    FLINK_GROUP: "Flink enrichment job, ais.raw",
    TRACKER_GROUP: "tracker, ais.enriched",
    "dvw-tracks-sink": "tracks sink, ais.tracks to dvw.tracks",
    "dvw-vessels-sink": "vessels sink, ais.tracks to PostGIS",
    ALERTS_SINK_GROUP: "alerts sink, ais.alerts to dvw.alerts",
}
# Only these topics count: the tracker's group also lists its changelog topic, which
# it never commits offsets on.
PIPELINE_TOPICS = frozenset({"ais.raw", "ais.enriched", "ais.tracks", "ais.alerts"})
PEAK_WINDOW_S = 30.0


@dataclass(frozen=True)
class GroupSample:
    consumed: int  # sum of committed offsets over the group's pipeline partitions
    lag: int  # sum of log end offset minus committed offset


@dataclass(frozen=True)
class Checkpoint:
    job_id: str
    id: int
    status: str  # COMPLETED, FAILED, IN_PROGRESS
    trigger_ms: int
    duration_ms: int


@dataclass(frozen=True)
class FlinkSample:
    job_id: str
    counts: dict[str, int]  # restored, total, in_progress, completed, failed
    history: tuple[Checkpoint, ...]
    restored_path: str | None  # external path of the state this job instance restored
    restored_id: int | None


@dataclass
class Sample:
    t: float  # epoch seconds
    groups: dict[str, GroupSample] = field(default_factory=dict)
    memory: dict[str, tuple[int, int]] = field(default_factory=dict)  # container -> (used, limit) bytes
    flink: FlinkSample | None = None


# ------------------------------------------------------------------ parsing


def parse_rpk_groups(text: str) -> dict[str, GroupSample]:
    """`rpk group describe ... --format json` -> per group, over PIPELINE_TOPICS."""
    out: dict[str, GroupSample] = {}
    for g in json.loads(text):
        consumed = lag = 0
        for p in g.get("partitions") or []:
            if p.get("topic") not in PIPELINE_TOPICS:
                continue
            current = int(p.get("current_offset", -1))
            consumed += max(current, 0)
            lag += max(int(p.get("lag", 0)), 0)
        out[str(g["group_name"])] = GroupSample(consumed, lag)
    return out


_UNITS = {
    "b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4,
    "kb": 1000, "mb": 1000**2, "gb": 1000**3, "tb": 1000**4,
}
_SIZE = re.compile(r"^\s*([0-9.]+)\s*([A-Za-z]+)\s*$")


def parse_size(text: str) -> int:
    """'68.86MiB' -> bytes."""
    m = _SIZE.match(text)
    if not m or m.group(2).lower() not in _UNITS:
        raise ValueError(f"not a size: {text!r}")
    return round(float(m.group(1)) * _UNITS[m.group(2).lower()])


def parse_docker_stats(text: str) -> dict[str, tuple[int, int]]:
    """`docker stats --no-stream --format '{{json .}}'` -> container -> (used, limit)."""
    out: dict[str, tuple[int, int]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        name = str(row.get("Name", ""))
        if not name.startswith(CONTAINER_PREFIX):
            continue
        used, _, limit = str(row.get("MemUsage", "")).partition("/")
        try:
            out[name] = (parse_size(used), parse_size(limit))
        except ValueError:
            continue  # "0B / 0B" style rows of a container that is just starting
    return out


def parse_checkpoints(job_id: str, doc: dict[str, Any]) -> FlinkSample:
    """GET /jobs/<id>/checkpoints -> FlinkSample."""
    history = tuple(
        Checkpoint(job_id, int(h["id"]), str(h["status"]), int(h.get("trigger_timestamp", 0)),
                   int(h.get("end_to_end_duration", 0)))
        for h in doc.get("history") or []
    )
    restored = (doc.get("latest") or {}).get("restored") or None
    return FlinkSample(
        job_id=job_id,
        counts={k: int(v) for k, v in (doc.get("counts") or {}).items()},
        history=history,
        restored_path=None if restored is None else str(restored.get("external_path")),
        restored_id=None if restored is None else int(restored["id"]),
    )


# ---------------------------------------------------------------- sources


def rpk_groups(groups: list[str]) -> dict[str, GroupSample]:
    out = subprocess.run(
        ["docker", "exec", REDPANDA_CONTAINER, "rpk", "group", "describe", *groups, "--format", "json"],
        capture_output=True, text=True, timeout=20, check=True,
    ).stdout
    return parse_rpk_groups(out)


def docker_memory() -> dict[str, tuple[int, int]]:
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
        capture_output=True, text=True, timeout=30, check=True,
    ).stdout
    return parse_docker_stats(out)


class FlinkRest:
    """The few REST calls the report needs, against the JobManager's web port."""

    def __init__(self, base_url: str, timeout_s: float = 3.0):
        self.base_url, self.timeout_s = base_url.rstrip("/"), timeout_s

    def get(self, path: str) -> Any:
        r = httpx.get(f"{self.base_url}{path}", timeout=self.timeout_s)
        r.raise_for_status()
        return r.json()

    def running_job(self) -> str | None:
        """Id of the running enrichment job, or None (none running, or no JobManager)."""
        try:
            jobs = self.get("/jobs/overview")["jobs"]
        except (httpx.HTTPError, KeyError, ValueError):
            return None
        for j in jobs:
            if j.get("name") == JOB_NAME and j.get("state") == "RUNNING":
                return str(j["jid"])
        return None

    def all_tasks_running(self, job_id: str) -> bool:
        try:
            tasks = self.get(f"/jobs/{job_id}")["status-counts"]
        except (httpx.HTTPError, KeyError, ValueError):
            return False
        return sum(int(v) for v in tasks.values()) == int(tasks.get("RUNNING", 0)) > 0

    def checkpoints(self, job_id: str) -> FlinkSample | None:
        try:
            return parse_checkpoints(job_id, self.get(f"/jobs/{job_id}/checkpoints"))
        except (httpx.HTTPError, KeyError, ValueError):
            return None

    def sample(self) -> FlinkSample | None:
        jid = self.running_job()
        return None if jid is None else self.checkpoints(jid)


# ---------------------------------------------------------------- sampler


class Sampler(threading.Thread):
    """Takes a Sample every interval_s until stop(); samples() returns them in order."""

    def __init__(self, flink: FlinkRest, interval_s: float = 5.0):
        super().__init__(name="report-sampler", daemon=True)
        self.flink, self.interval_s = flink, interval_s
        self._halt = threading.Event()
        self._lock = threading.Lock()
        self._samples: list[Sample] = []
        self.errors: dict[str, int] = {"rpk": 0, "docker": 0}

    def take(self) -> Sample:
        s = Sample(time.time())
        try:
            s.groups = rpk_groups(list(STAGES))
        except (subprocess.SubprocessError, ValueError, KeyError):
            self.errors["rpk"] += 1
        s.flink = self.flink.sample()
        try:
            s.memory = docker_memory()
        except (subprocess.SubprocessError, ValueError):
            self.errors["docker"] += 1
        with self._lock:
            self._samples.append(s)
        return s

    def run(self) -> None:
        next_at = time.monotonic()
        while not self._halt.is_set():
            self.take()
            next_at += self.interval_s
            self._halt.wait(max(0.0, next_at - time.monotonic()))

    def stop(self) -> None:
        self._halt.set()
        self.join()

    def samples(self) -> list[Sample]:
        with self._lock:
            return list(self._samples)


# ---------------------------------------------------------------- summaries


@dataclass(frozen=True)
class Throughput:
    records: int
    seconds: float
    avg_per_s: float
    peak_per_s: float  # best PEAK_WINDOW_S window


def throughput(samples: list[Sample], group: str, window_s: float = PEAK_WINDOW_S) -> Throughput | None:
    """Records the group committed over the samples, and the rate while it was moving:
    from the last sample before its first progress to the sample of its last progress.
    None when the group never moved."""
    pts = [(s.t, s.groups[group].consumed) for s in samples if group in s.groups]
    moved = [i for i in range(1, len(pts)) if pts[i][1] > pts[i - 1][1]]
    if not moved:
        return None
    a, b = moved[0] - 1, moved[-1]
    records = pts[b][1] - pts[a][1]
    seconds = pts[b][0] - pts[a][0]
    peak = 0.0
    j = a
    for i in range(a, b + 1):
        while j <= b and pts[j][0] - pts[i][0] < window_s:
            j += 1
        if j > b:
            break
        peak = max(peak, (pts[j][1] - pts[i][1]) / (pts[j][0] - pts[i][0]))
    if peak == 0.0 and seconds > 0:  # active for less than one window
        peak = records / seconds
    return Throughput(records, seconds, records / seconds if seconds > 0 else 0.0, peak)


def lag_summary(samples: list[Sample]) -> dict[str, tuple[int, int]]:
    """group -> (max lag over the samples, lag in the last sample that has the group)."""
    out: dict[str, tuple[int, int]] = {}
    for s in samples:
        for g, gs in s.groups.items():
            top, _ = out.get(g, (0, 0))
            out[g] = (max(top, gs.lag), gs.lag)
    return out


@dataclass(frozen=True)
class Memory:
    peak: dict[str, int]  # container -> peak bytes
    limit: dict[str, int]  # container -> limit bytes
    peak_total: int  # highest sum over the containers of one sample
    final_total: int


def memory_summary(samples: list[Sample]) -> Memory:
    peak: dict[str, int] = {}
    limit: dict[str, int] = {}
    totals: list[int] = []
    for s in samples:
        if not s.memory:
            continue
        for name, (used, lim) in s.memory.items():
            peak[name] = max(peak.get(name, 0), used)
            limit[name] = lim
        totals.append(sum(used for used, _ in s.memory.values()))
    return Memory(peak, limit, max(totals, default=0), totals[-1] if totals else 0)


@dataclass(frozen=True)
class Checkpoints:
    completed: int  # from the counters: every checkpoint completed in the window
    failed: int
    restores: int  # times a job instance restored state (a failover, or a submit from state)
    sampled: int  # completed checkpoints whose duration was seen in the history
    avg_ms: float | None
    max_ms: int | None
    jobs: tuple[str, ...]  # job instances seen, in order
    restored: dict[str, str]  # job id -> state path it restored, for jobs started in the window


def checkpoint_summary(samples: list[Sample]) -> Checkpoints:
    """Counters are deltas per job instance: from its first sample for the job running
    when sampling started, from zero for one submitted later. Durations come from the
    history each sample returns (the last 10 checkpoints), the latest status per id."""
    first: dict[str, dict[str, int]] = {}
    last: dict[str, dict[str, int]] = {}
    seen: dict[tuple[str, int], Checkpoint] = {}
    restored: dict[str, str] = {}
    order: list[str] = []
    initial = next((s.flink.job_id for s in samples if s.flink is not None), None)
    for s in samples:
        f = s.flink
        if f is None:
            continue
        if f.job_id not in first:
            order.append(f.job_id)
            started_before = f.job_id == initial
            first[f.job_id] = dict(f.counts) if started_before else {}
            if not started_before and f.restored_path:
                restored[f.job_id] = f.restored_path
        last[f.job_id] = dict(f.counts)
        for c in f.history:
            seen[(c.job_id, c.id)] = c

    def delta(key: str) -> int:
        return sum(last[j].get(key, 0) - first[j].get(key, 0) for j in last)

    # History entries from before sampling started are not part of the window.
    t0_ms = int(samples[0].t * 1000) if samples else 0
    durations = [c.duration_ms for c in seen.values() if c.status == "COMPLETED" and c.trigger_ms >= t0_ms]
    return Checkpoints(
        completed=delta("completed"),
        failed=delta("failed"),
        restores=delta("restored"),
        sampled=len(durations),
        avg_ms=sum(durations) / len(durations) if durations else None,
        max_ms=max(durations) if durations else None,
        jobs=tuple(order),
        restored=restored,
    )
