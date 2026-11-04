from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from reconcile.run_check import Produced, RunError, RunStillRunning, Stored, compare, load_produced

RUN = "20261102T132701Z-6cdc0c"


def produced_json(
    tmp_path: Path, status: str | None = "complete", run_id: str = RUN, partitions: dict[str, Any] | None = None
) -> Path:
    partitions = partitions or {"0": {"first_offset": 0, "last_offset": 9, "count": 10},
                           "1": {"first_offset": 5, "last_offset": 7, "count": 3}}
    path = tmp_path / RUN / "produced.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"run_id": run_id, "status": status, "partitions": partitions}))
    return path


def test_load_produced_reads_partitions(tmp_path: Path) -> None:
    assert load_produced(produced_json(tmp_path), RUN) == [Produced(0, 0, 9, 10), Produced(1, 5, 7, 3)]


@pytest.mark.parametrize("status", ["interrupted", "failed", None])
def test_load_produced_refuses_incomplete_runs(tmp_path: Path, status: str | None) -> None:
    with pytest.raises(RunError, match="only complete runs"):
        load_produced(produced_json(tmp_path, status=status), RUN)


def test_load_produced_refuses_other_run_and_bad_ranges(tmp_path: Path) -> None:
    with pytest.raises(RunError, match="not"):
        load_produced(produced_json(tmp_path, run_id="other"), RUN)
    bad = {"0": {"first_offset": 0, "last_offset": 1, "count": 5}}
    with pytest.raises(RunError, match="inconsistent"):
        load_produced(produced_json(tmp_path / "x", partitions=bad), RUN)


def test_compare_all_match() -> None:
    produced = [Produced(0, 0, 9, 10), Produced(1, 5, 7, 3)]
    stored = {0: Stored(10, 0, 10), 1: Stored(3, 0, 6)}  # duplicates not merged yet: still OK
    results = compare(produced, stored)
    assert all(r.ok for r in results)


def test_compare_flags_missing_partition_and_rows() -> None:
    produced = [Produced(0, 0, 9, 10), Produced(1, 5, 7, 3)]
    results = compare(produced, {0: Stored(8, 0, 8)})
    assert [r.ok for r in results] == [False, False]
    assert results[0].note == "missing 2"
    assert results[1].note == "missing 3"  # nothing stored at all for partition 1


def test_compare_flags_rows_from_another_run() -> None:
    # Right count, wrong owner: e.g. the topic was recreated and offsets reused.
    results = compare([Produced(0, 0, 9, 10)], {0: Stored(10, 2, 10)})
    assert not results[0].ok
    assert results[0].note == "2 rows from another run"


def test_compare_flags_extra_rows() -> None:
    # Another producer wrote into the run's offset range while it was running.
    results = compare([Produced(0, 0, 9, 9)], {0: Stored(10, 1, 10)})
    assert results[0].note == "extra 1, 1 rows from another run"


def test_a_running_run_is_its_own_error_type(tmp_path: Path) -> None:
    """The reconciliation job waits on this type, not on the wording of the message."""
    with pytest.raises(RunStillRunning):
        load_produced(produced_json(tmp_path / "a", status="running"), RUN)
    with pytest.raises(RunError) as failed:
        load_produced(produced_json(tmp_path / "b", status="failed"), RUN)
    assert not isinstance(failed.value, RunStillRunning)
