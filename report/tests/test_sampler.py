from __future__ import annotations

import json

import pytest

from report.sampler import (
    Checkpoint,
    FlinkSample,
    GroupSample,
    Sample,
    checkpoint_summary,
    lag_summary,
    memory_summary,
    parse_checkpoints,
    parse_docker_stats,
    parse_rpk_groups,
    parse_size,
    throughput,
)

MIB = 1024**2


def test_rpk_groups_sum_pipeline_partitions_only() -> None:
    doc = [
        {"group_name": "dvw-tracker", "partitions": [
            {"topic": "ais.enriched", "partition": 0, "current_offset": 100, "log_end_offset": 104, "lag": 4},
            {"topic": "ais.enriched", "partition": 1, "current_offset": 50, "log_end_offset": 50, "lag": 0},
            # The changelog is listed with no commit and its whole length as lag; it must not count.
            {"topic": "dvw-tracker-vessel_state-changelog", "partition": 0, "current_offset": -1,
             "log_end_offset": 1461, "lag": 1461},
        ]},
        {"group_name": "dvw-alerts-sink", "partitions": [
            {"topic": "ais.alerts", "partition": 0, "current_offset": -1, "log_end_offset": 0, "lag": 0},
        ]},
    ]
    got = parse_rpk_groups(json.dumps(doc))
    assert got == {"dvw-tracker": GroupSample(150, 4), "dvw-alerts-sink": GroupSample(0, 0)}


@pytest.mark.parametrize("text,expected", [
    ("68.86MiB", round(68.86 * MIB)), ("1.25GiB", round(1.25 * 1024**3)), ("512MiB", 512 * MIB),
    ("0B", 0), ("12.5kB", 12500),
])
def test_parse_size(text: str, expected: int) -> None:
    assert parse_size(text) == expected


def test_parse_size_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        parse_size("lots")


def test_docker_stats_keeps_dvw_containers() -> None:
    lines = "\n".join(json.dumps(r) for r in [
        {"Name": "dvw-tracker", "MemUsage": "68.86MiB / 512MiB"},
        {"Name": "other", "MemUsage": "1GiB / 2GiB"},
        {"Name": "dvw-starting", "MemUsage": "-- / --"},
    ])
    assert parse_docker_stats(lines) == {"dvw-tracker": (round(68.86 * MIB), 512 * MIB)}


def test_parse_checkpoints() -> None:
    doc = {
        "counts": {"restored": 1, "total": 5, "in_progress": 0, "completed": 4, "failed": 1},
        "history": [{"id": 9, "status": "COMPLETED", "trigger_timestamp": 1000, "end_to_end_duration": 61}],
        "latest": {"restored": {"id": 3, "external_path": "file:/flink-data/checkpoints/abc/chk-3"}},
    }
    f = parse_checkpoints("j1", doc)
    assert f.counts["completed"] == 4
    assert f.history == (Checkpoint("j1", 9, "COMPLETED", 1000, 61),)
    assert (f.restored_path, f.restored_id) == ("file:/flink-data/checkpoints/abc/chk-3", 3)
    assert parse_checkpoints("j1", {"counts": {}, "latest": {"restored": None}}).restored_path is None


def g(consumed: int, lag: int = 0) -> dict[str, GroupSample]:
    return {"grp": GroupSample(consumed, lag)}


def test_throughput_over_the_moving_span() -> None:
    # Idle, then 100/s for 60 s (with a 30 s burst at 200/s inside), then idle again.
    pts = [(0, 1000), (10, 1000), (20, 2000), (30, 4000), (40, 6000), (50, 7000), (60, 7000), (70, 7000)]
    samples = [Sample(float(t), groups=g(c)) for t, c in pts]
    tp = throughput(samples, "grp")
    assert tp is not None
    assert tp.records == 6000
    assert tp.seconds == 40.0  # from the last idle sample (10 s) to the last progress (50 s)
    assert tp.avg_per_s == 150.0
    assert tp.peak_per_s == 5000 / 30  # 10 s to 40 s


def test_throughput_none_when_the_group_never_moves() -> None:
    assert throughput([Sample(0.0, groups=g(5)), Sample(5.0, groups=g(5))], "grp") is None
    assert throughput([Sample(0.0)], "grp") is None


def test_throughput_shorter_than_one_window() -> None:
    tp = throughput([Sample(0.0, groups=g(0)), Sample(5.0, groups=g(500))], "grp")
    assert tp is not None and tp.peak_per_s == tp.avg_per_s == 100.0


def test_lag_max_and_final() -> None:
    samples = [Sample(0.0, groups=g(0, 5)), Sample(5.0, groups=g(0, 900)), Sample(10.0, groups=g(0, 4)),
               Sample(15.0)]  # a sample where rpk failed does not reset the final value
    assert lag_summary(samples) == {"grp": (900, 4)}


def test_memory_peak_total_is_one_sample_not_sum_of_peaks() -> None:
    samples = [
        Sample(0.0, memory={"a": (100, 1000), "b": (10, 500)}),
        Sample(5.0, memory={"a": (10, 1000), "b": (400, 500)}),
        Sample(10.0),  # docker stats failed
    ]
    m = memory_summary(samples)
    assert m.peak == {"a": 100, "b": 400}
    assert m.limit == {"a": 1000, "b": 500}
    assert m.peak_total == 410
    assert m.final_total == 410


def flink(job: str, completed: int, failed: int, restored: int, history: list[tuple[int, str, int, int]],
          path: str | None = None) -> FlinkSample:
    return FlinkSample(job, {"completed": completed, "failed": failed, "restored": restored},
                       tuple(Checkpoint(job, i, st, trig, dur) for i, st, trig, dur in history), path, None)


def test_checkpoints_across_a_resubmitted_job() -> None:
    samples = [
        # The job running when sampling starts: counters are deltas from here.
        Sample(100.0, flink=flink("old", 50, 2, 1, [(49, "COMPLETED", 99_000, 900)])),
        Sample(105.0, flink=flink("old", 51, 3, 2, [(50, "IN_PROGRESS", 101_000, 0)])),
        Sample(110.0, flink=flink("old", 52, 3, 2, [(50, "COMPLETED", 101_000, 300),
                                                     (51, "COMPLETED", 104_000, 100)])),
        Sample(115.0),  # JobManager down
        # Submitted from the old job's checkpoint: counters count from zero.
        Sample(120.0, flink=flink("new", 2, 0, 1, [(52, "COMPLETED", 118_000, 2000)],
                                  "file:/flink-data/checkpoints/old/chk-51")),
    ]
    c = checkpoint_summary(samples)
    assert (c.completed, c.failed, c.restores) == (2 + 2, 1, 1 + 1)
    # Checkpoint 49 was triggered before sampling started; 50's last status is COMPLETED.
    assert c.sampled == 3
    assert c.avg_ms == (300 + 100 + 2000) / 3
    assert c.max_ms == 2000
    assert c.jobs == ("old", "new")
    assert c.restored == {"new": "file:/flink-data/checkpoints/old/chk-51"}


def test_checkpoints_without_flink_samples() -> None:
    c = checkpoint_summary([Sample(0.0)])
    assert (c.completed, c.sampled, c.avg_ms, c.max_ms, c.jobs) == (0, 0, None, None, ())
