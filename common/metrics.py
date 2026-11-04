"""Prometheus metrics for the long-running Python services, off unless METRICS_PORT is set.

The sinks and the tracker already count and time their work for their log lines. With
METRICS_PORT set, the same numbers are also served on http://0.0.0.0:<port>/metrics.
Without it (the default, and what `make up` runs), prometheus_client is never imported,
no thread or port is opened, and every hook here returns at once. So the local stack's
memory does not change, and the deploy kit turns metrics on per service.

Only the environment is read, not .env: a host process (make replay, make report) must
not open a port because of a setting meant for containers.

Metric names: dvw_sink_* carry a `sink` label (raw, alerts, tracks, vessels), so one
dashboard panel covers every sink; dvw_tracker_* come from the one tracker worker.
"""

from __future__ import annotations

import os
import threading
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry, Counter as PromCounter, Gauge, Histogram
    from prometheus_client.core import Metric

ENV_PORT = "METRICS_PORT"

# Batch writes take milliseconds when healthy and seconds when ClickHouse is merging or a
# retry backs off (BatchSettings: up to 10 s per attempt).
_WRITE_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
_BATCH_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)


@dataclass(frozen=True)
class _Families:
    registry: CollectorRegistry
    sink_records: PromCounter
    sink_rows: PromCounter
    sink_skipped: PromCounter
    sink_results: PromCounter
    sink_write_seconds: Histogram
    sink_commit_seconds: Histogram
    sink_write_retries: PromCounter
    sink_commit_failures: PromCounter
    sink_last_commit: Gauge
    tracker_batch_seconds: Histogram
    tracker_batch_records: Histogram


_families: _Families | None = None
_lock = threading.Lock()


def enabled() -> bool:
    return _families is not None


def enable(registry: CollectorRegistry | None = None) -> None:
    """Create the metric families (idempotent). serve_from_env() calls this; tests pass
    their own registry."""
    global _families
    from prometheus_client import REGISTRY, Counter as PromCounter, Gauge, Histogram

    with _lock:
        if _families is not None:
            return
        reg = registry if registry is not None else REGISTRY
        s = ("sink",)
        _families = _Families(
            registry=reg,
            sink_records=PromCounter("dvw_sink_records", "Records consumed and committed", s, registry=reg),
            sink_rows=PromCounter("dvw_sink_rows", "Rows written to the store", s, registry=reg),
            sink_skipped=PromCounter("dvw_sink_skipped", "Records committed without a row", s, registry=reg),
            sink_results=PromCounter("dvw_sink_write_results", "Per-write counters the sink reports",
                                     ("sink", "result"), registry=reg),
            sink_write_seconds=Histogram("dvw_sink_write_seconds", "Batch write time, retries included",
                                         s, buckets=_WRITE_BUCKETS, registry=reg),
            sink_commit_seconds=Histogram("dvw_sink_commit_seconds", "Synchronous offset commit time",
                                          s, buckets=_WRITE_BUCKETS, registry=reg),
            sink_write_retries=PromCounter("dvw_sink_write_retries", "Failed write attempts that were retried",
                                           s, registry=reg),
            sink_commit_failures=PromCounter("dvw_sink_commit_failures",
                                             "Offset commits that failed after a successful write", s, registry=reg),
            sink_last_commit=Gauge("dvw_sink_last_commit_timestamp_seconds",
                                   "Unix time of the last batch commit", s, registry=reg),
            tracker_batch_seconds=Histogram("dvw_tracker_batch_seconds",
                                            "Time to process one batch: tracks, acks, state writes",
                                            buckets=_BATCH_BUCKETS, registry=reg),
            tracker_batch_records=Histogram("dvw_tracker_batch_records", "Records per batch",
                                            buckets=(1, 10, 50, 100, 250, 500, 1000, 2500, 5000), registry=reg),
        )


def disable() -> None:
    """Forget the families (tests only; the registry keeps what was registered)."""
    global _families
    with _lock:
        _families = None


def serve_from_env(environ: Mapping[str, str] = os.environ,
                   registry: CollectorRegistry | None = None) -> int | None:
    """Serve /metrics on METRICS_PORT from a daemon thread. Returns the port, or None
    when METRICS_PORT is unset or empty (metrics off)."""
    raw = environ.get(ENV_PORT, "").strip()
    if not raw:
        return None
    port = int(raw)
    from prometheus_client import start_http_server

    enable(registry)
    assert _families is not None
    start_http_server(port, registry=_families.registry)
    return port


class SinkMetrics:
    """One sink's hooks. Cheap to create; a no-op while metrics are off."""

    def __init__(self, sink: str) -> None:
        self.sink = sink
        f = _families
        if f is not None:
            # Create the labelled series at 0, so a sink with no traffic yet still shows up.
            for fam in (f.sink_records, f.sink_rows, f.sink_skipped, f.sink_write_retries, f.sink_commit_failures):
                fam.labels(sink)

    def batch(self, records: int, rows: int, skipped: int, write_s: float, commit_s: float,
              results: Mapping[str, int], committed_at: float) -> None:
        f = _families
        if f is None:
            return
        f.sink_records.labels(self.sink).inc(records)
        f.sink_rows.labels(self.sink).inc(rows)
        f.sink_skipped.labels(self.sink).inc(skipped)
        for name, n in results.items():
            f.sink_results.labels(self.sink, name).inc(n)
        if rows:
            f.sink_write_seconds.labels(self.sink).observe(write_s)
        f.sink_commit_seconds.labels(self.sink).observe(commit_s)
        f.sink_last_commit.labels(self.sink).set(committed_at)

    def write_retry(self) -> None:
        if _families is not None:
            _families.sink_write_retries.labels(self.sink).inc()

    def commit_failed(self) -> None:
        if _families is not None:
            _families.sink_commit_failures.labels(self.sink).inc()


def tracker_batch(seconds: float, records: int) -> None:
    f = _families
    if f is None:
        return
    f.tracker_batch_seconds.observe(seconds)
    f.tracker_batch_records.observe(records)


def export_counts(name: str, documentation: str, label: str, counts: Counter[str]) -> None:
    """Serve a live Counter as one Prometheus counter family, one series per key.

    The tracker keeps its outcome counts (read, accepted, rejected per reason, alerts per
    type, Flink leaks) in a Counter for its log line; this exposes the same Counter
    instead of keeping a second copy in step. No-op while metrics are off.
    """
    f = _families
    if f is None:
        return
    from prometheus_client.core import CounterMetricFamily
    from prometheus_client.registry import Collector

    class CountsCollector(Collector):
        def collect(self) -> Iterator[Metric]:
            fam = CounterMetricFamily(name, documentation, labels=[label])
            # list() copies the items in one C call while holding the GIL, so the event
            # loop thread cannot resize the dict mid-iteration.
            for key, value in sorted(list(counts.items())):
                fam.add_metric([key], value)
            yield fam

    f.registry.register(CountsCollector())
