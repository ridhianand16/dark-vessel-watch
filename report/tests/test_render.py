from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from report.chaos import Fault, flink_recovered
from report.collect import CheckRow, Copies, Latencies, Reconciliation, Totals
from report.environment import Environment, Image
from report.render import (
    INDEX_END,
    INDEX_START,
    Report,
    index_line,
    is_checkpoint,
    render,
    report_path,
    state,
    update_index,
)
from report.sampler import Checkpoint, Checkpoints, FlinkSample, Memory

CHK = "file:/flink-data/checkpoints/e5bc636eb968001794a98dbd588078bc/chk-23"


@pytest.mark.parametrize("path,text,checkpoint", [
    (CHK, "checkpoint 23 of job `e5bc636eb968001794a98dbd588078bc`", True),
    ("file:/flink-data/savepoints/savepoint-ab12cd-0123456789ab", "savepoint", False),
    (None, "nothing (the job started fresh)", False),
])
def test_restored_state(path: str | None, text: str, checkpoint: bool) -> None:
    assert text in state(path)
    assert is_checkpoint(path) is checkpoint


def env() -> Environment:
    return Environment(datetime(2026, 11, 4, 12, 0, tzinfo=UTC), "1450308abc", "1450308", False, "M1", "macOS 26.5",
                       "OrbStack", (Image("dvw-tracker", "dvw-tracker:0.1.0", "a1b2c3d4e5f6", True),),
                       "2026-10-28", "zones-ee4167a002e7")


def test_report_path_does_not_overwrite(tmp_path: Path) -> None:
    first = report_path(tmp_path, env())
    assert first.name == "2026-11-04-1450308.md"
    first.write_text("x")
    assert report_path(tmp_path, env()).name == "2026-11-04-1450308-2.md"


def test_update_index_puts_newest_first(tmp_path: Path) -> None:
    index = tmp_path / "results.md"
    index.write_text(f"# Results\n\nintro\n\n{INDEX_START}\n{INDEX_END}\n\n## Older\n\nkept\n")
    update_index(index, "- one")
    update_index(index, "- two")
    text = index.read_text()
    assert f"{INDEX_START}\n- two\n- one\n{INDEX_END}" in text
    assert text.endswith("## Older\n\nkept\n")


def test_update_index_needs_markers(tmp_path: Path) -> None:
    index = tmp_path / "results.md"
    index.write_text("# Results\n")
    with pytest.raises(ValueError):
        update_index(index, "- one")


def report(faults: list[Fault], failed: bool = False) -> Report:
    checks = (CheckRow(0, "raw_stored", "fail" if failed else "ok", "missing 1" if failed else "ok", 10, 9),
              CheckRow(1, "raw_stored", "ok", "ok", 5, 5))
    return Report(
        command="make report ROWS=700000 CHAOS=1", env=env(), run_id="20261104T120000Z-abcdef", rows=700_000,
        speed=10.0, produced={"status": "complete", "sent": 701_665, "elapsed_s": 340.2,
                              "started_at": "2026-11-04T12:00:00.000+00:00"},
        injected=True, inject_note="", faults=faults, chaos_error=None, throughput={}, lag={},
        memory=Memory({}, {}, 0, 0),
        checkpoints=Checkpoints(0, 0, 0, 0, None, None, (), {}),
        latencies=Latencies(None, None, None),
        reconciliation=Reconciliation(checks, Totals(15, 15, 9, 1, 5, 0, 8, 8), {"no_position": 1}),
        reconcile_rc=1 if failed else 0, reconcile_output="reconcile: OK", detection=None, score_rc=0,
        score_output="", copies=[Copies("dvw.alerts", "alert_id", 30, 24, 24)],
        finished_at=datetime(2026, 11, 4, 12, 15, tzinfo=UTC),
    )


def jm_fault(restored: str | None) -> Fault:
    t = datetime(2026, 11, 4, 12, 4, tzinfo=UTC).timestamp()
    return Fault("jobmanager", t, t + 10, t + 60, restored, 23, "flinkctl: starting dvw-enrichment from x",
                 job_before="e5bc636eb968001794a98dbd588078bc", job_after="ffff")


def test_render_states_the_checkpoint_restored_after_the_jobmanager_kill() -> None:
    text = render(report([jm_fault(CHK)]))
    assert text.startswith("# Results report, 2026-11-04, `1450308`\n\n```sh\nmake report ROWS=700000 CHAOS=1\n```")
    assert "`make flink-up` restored checkpoint 23 of job `e5bc636eb968001794a98dbd588078bc` (the killed job)" in text
    assert "found no loss" in text
    assert "flinkctl: starting dvw-enrichment from x" in text


def test_render_says_so_when_the_job_started_fresh() -> None:
    text = render(report([jm_fault(None)]))
    assert "did NOT restore a checkpoint; it restored nothing (the job started fresh)" in text


def test_failed_reconciliation_is_in_summary_and_index() -> None:
    r = report([], failed=True)
    assert "Reconciliation FAILED: 1 checks failed (raw_stored)" in render(r)
    assert "reconciliation FAILED" in index_line(r, Path("docs/results/x.md"))
    assert not r.ok


class FakeFlink:
    def __init__(self, job: str | None, running: bool, history: list[Checkpoint]):
        self.job, self.running, self.history = job, running, history

    def running_job(self) -> str | None:
        return self.job

    def all_tasks_running(self, job_id: str) -> bool:
        return self.running

    def checkpoints(self, job_id: str) -> FlinkSample:
        return FlinkSample(job_id, {}, tuple(self.history), CHK, 23)


def test_flink_recovered_needs_a_checkpoint_after_the_restart() -> None:
    before = Checkpoint("j", 1, "COMPLETED", 1_000, 50)
    after = Checkpoint("j", 2, "COMPLETED", 3_000, 50)
    assert flink_recovered(FakeFlink("j", True, [before]), 2_000) is None  # type: ignore[arg-type]
    assert flink_recovered(FakeFlink("j", False, [before, after]), 2_000) is None  # type: ignore[arg-type]
    assert flink_recovered(FakeFlink("j", True, [before, after]), 2_000) == ("j", 2, CHK, 23)  # type: ignore[arg-type]
    # After a JobManager kill only a new job instance counts.
    assert flink_recovered(FakeFlink("j", True, [after]), 2_000, old_job="j") is None  # type: ignore[arg-type]
    assert flink_recovered(FakeFlink(None, False, []), 2_000) is None  # type: ignore[arg-type]
