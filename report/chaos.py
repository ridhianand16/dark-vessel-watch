"""The faults a CHAOS=1 report injects while the replay runs, and their recovery.

In this order, each one only after the one before has recovered:

    taskmanager  SIGKILL the Flink TaskManager, start it again after a pause. The job
                 fails over by itself and restores its last checkpoint.
    tracker      SIGKILL the tracker, start it again. It resumes from its committed
                 offsets and its changelog-backed state (ADR 0004).
    jobmanager   SIGKILL the Flink JobManager, then `make flink-up`, which starts it
                 and submits the job from the newest retained checkpoint (ADR 0022).

The JobManager is killed, not stopped: a graceful stop suspends the job, and without
high availability Flink then deletes that job's checkpoints, so `make flink-up` would
start fresh rather than from a checkpoint (ADR 0022, ADR 0023).

Recovered means: for Flink, the job is RUNNING with every task running and has
completed a checkpoint triggered after the restart; for the tracker, its consumer
group has committed new offsets.
"""

from __future__ import annotations

import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass

from common.env import REPO_ROOT
from reconcile.kafka import TRACKER_GROUP
from report.sampler import FlinkRest, rpk_groups

TASKMANAGER = "dvw-flink-taskmanager"
JOBMANAGER = "dvw-flink-jobmanager"
TRACKER = "dvw-tracker"
# Where in the replay each fault starts, as a share of its expected duration.
AT = {"taskmanager": 0.20, "tracker": 0.45, "jobmanager": 0.70}
DOWN_S = {"taskmanager": 15.0, "tracker": 10.0, "jobmanager": 10.0}
RECOVER_TIMEOUT_S = 300.0


class ChaosError(Exception):
    pass


@dataclass
class Fault:
    name: str
    killed_at: float  # epoch s
    started_at: float  # epoch s, when the container was started again (or flink-up began)
    recovered_at: float | None = None
    restored: str | None = None  # Flink: path of the state the job restored
    restored_id: int | None = None
    output: str = ""  # make flink-up output, for the jobmanager fault
    job_before: str | None = None  # Flink job id before the fault
    job_after: str | None = None  # and once recovered

    @property
    def outage_s(self) -> float | None:
        return None if self.recovered_at is None else self.recovered_at - self.killed_at


def log(msg: str) -> None:
    print(f"report: {msg}", file=sys.stderr, flush=True)


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True, text=True, timeout=60)


def wait_until(cond: Callable[[], bool], timeout_s: float, poll_s: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(poll_s)
    return False


def flink_recovered(flink: FlinkRest, after_ms: int, old_job: str | None = None) -> tuple[str, int, str | None, int | None] | None:
    """(job id, checkpoint id, restored path, restored id) once the job runs every task and
    has completed a checkpoint triggered after `after_ms`; None until then. With old_job,
    only a different job instance counts (a resubmitted job)."""
    jid = flink.running_job()
    if jid is None or jid == old_job or not flink.all_tasks_running(jid):
        return None
    cp = flink.checkpoints(jid)
    if cp is None:
        return None
    done = [c for c in cp.history if c.status == "COMPLETED" and c.trigger_ms > after_ms]
    if not done:
        return None
    return jid, max(c.id for c in done), cp.restored_path, cp.restored_id


def kill_taskmanager(flink: FlinkRest) -> Fault:
    before = flink.running_job()
    log(f"fault: SIGKILL {TASKMANAGER}")
    # The fault's window opens before the kill, so a sample taken while `docker kill`
    # runs is inside it and not reported as an unplanned restart.
    f = Fault("taskmanager", time.time(), 0.0, job_before=before)
    docker("kill", TASKMANAGER)
    time.sleep(DOWN_S["taskmanager"])
    docker("start", TASKMANAGER)
    f.started_at = time.time()
    got: list[tuple[str, int, str | None, int | None]] = []

    def done() -> bool:
        r = flink_recovered(flink, int(f.started_at * 1000))
        if r:
            got.append(r)
        return r is not None

    if not wait_until(done, RECOVER_TIMEOUT_S):
        raise ChaosError(f"the Flink job did not recover within {RECOVER_TIMEOUT_S:.0f} s of starting {TASKMANAGER}")
    f.recovered_at = time.time()
    f.job_after, _, f.restored, f.restored_id = got[-1]
    log(f"recovered: job restored {f.restored} in {f.outage_s:.0f} s")
    return f


def kill_tracker() -> Fault:
    before = rpk_groups([TRACKER_GROUP])[TRACKER_GROUP].consumed
    log(f"fault: SIGKILL {TRACKER}")
    f = Fault("tracker", time.time(), 0.0)
    docker("kill", TRACKER)
    time.sleep(DOWN_S["tracker"])
    docker("start", TRACKER)
    f.started_at = time.time()
    if not wait_until(lambda: rpk_groups([TRACKER_GROUP])[TRACKER_GROUP].consumed > before, RECOVER_TIMEOUT_S, 2.0):
        raise ChaosError(f"the tracker committed nothing within {RECOVER_TIMEOUT_S:.0f} s of starting it")
    f.recovered_at = time.time()
    log(f"recovered: tracker committing again after {f.outage_s:.0f} s")
    return f


def kill_jobmanager(flink: FlinkRest) -> Fault:
    old = flink.running_job()
    log(f"fault: SIGKILL {JOBMANAGER} (job {old})")
    f = Fault("jobmanager", time.time(), 0.0, job_before=old)
    docker("kill", JOBMANAGER)
    time.sleep(DOWN_S["jobmanager"])
    f.started_at = time.time()
    log("recovering: make flink-up")
    proc = subprocess.run(["make", "flink-up"], cwd=REPO_ROOT, capture_output=True, text=True, timeout=600)
    f.output = (proc.stdout + proc.stderr).strip()
    sys.stderr.write(f.output + "\n")
    if proc.returncode != 0:
        raise ChaosError(f"make flink-up failed with exit code {proc.returncode}")
    got: list[tuple[str, int, str | None, int | None]] = []

    def done() -> bool:
        r = flink_recovered(flink, int(f.started_at * 1000), old_job=old)
        if r:
            got.append(r)
        return r is not None

    if not wait_until(done, RECOVER_TIMEOUT_S):
        raise ChaosError(f"no new Flink job ran and checkpointed within {RECOVER_TIMEOUT_S:.0f} s of make flink-up")
    f.recovered_at = time.time()
    f.job_after, _, f.restored, f.restored_id = got[-1]
    log(f"recovered: new job restored {f.restored or 'nothing (fresh start)'} in {f.outage_s:.0f} s")
    return f


def run(
    flink: FlinkRest, replay_started: float, expected_s: float, replay_done: Callable[[], bool], faults: list[Fault]
) -> None:
    """Inject the three faults at their share of the expected replay time, appending each
    to `faults` as it recovers. Raises ChaosError when a fault would come after the
    replay finished, or does not recover; the faults before it stay in the list."""
    steps: list[tuple[str, Callable[[], Fault]]] = [
        ("taskmanager", lambda: kill_taskmanager(flink)),
        ("tracker", kill_tracker),
        ("jobmanager", lambda: kill_jobmanager(flink)),
    ]
    for name, step in steps:
        at = replay_started + AT[name] * expected_s
        while time.time() < at and not replay_done():
            time.sleep(0.5)
        if replay_done():
            raise ChaosError(f"the replay finished before the {name} fault; raise ROWS")
        faults.append(step())
