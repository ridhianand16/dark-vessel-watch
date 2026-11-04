"""One command that replays at 10x, optionally kills components, and writes a report.

    make report [ROWS=700000] [CHAOS=1]
    uv run python -m report --csv data/ais/aisdk-<day>.csv --rows 700000 [--chaos]

1. Records the environment: commit, machine, Docker VM, images, AIS day, zone package.
2. Replays ROWS rows at 10x with `make replay`, with the labelled anomalies of
   ingest/replayer/anomalies.yaml when the rows cover the event time they need
   (54 min, about 650,000 rows); otherwise without, and detection is not scored.
3. Samples consumer lag, memory and Flink checkpoints every 5 s (report/sampler.py).
4. With --chaos, kills the Flink TaskManager, the tracker and the Flink JobManager
   partway through, recovering each (report/chaos.py).
5. Runs `make reconcile-run`, which waits until every stage has finished the run, then
   `make score` for an injected run, and reads latency, duplicates and the verdicts
   from ClickHouse.
6. Writes docs/results/<date>-<short sha>.md and adds it to the index in
   docs/results.md.

A report is written whatever the outcome; a failed check is in it, not hidden.
Exit codes: 0 every check passed; 1 a check failed, a fault did not recover, or
the replay failed; 2 the stack or the input is not ready.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import IO

from common.clickhouse import client_from_env
from common.env import REPO_ROOT, setting
from ingest.replayer import inject
from ingest.replayer.source import SourceError, event_span, read_layout
from report import chaos, collect, environment, render, sampler
from report.chaos import ChaosError, Fault, log

SPEED = 10.0
RUNS_DIR = REPO_ROOT / "data" / "runs"
OUT_DIR = REPO_ROOT / "docs" / "results"
INDEX = REPO_ROOT / "docs" / "results.md"
ANOMALIES = REPO_ROOT / "ingest" / "replayer" / "anomalies.yaml"
RUNNING = ("dvw-redpanda", "dvw-clickhouse", "dvw-postgis", "dvw-raw-sink", "dvw-tracks-alerts-sink",
           "dvw-vessels-sink", "dvw-tracker", "dvw-flink-jobmanager", "dvw-flink-taskmanager")
MIN_CHAOS_REPLAY_S = 120.0


def stream(proc: subprocess.Popen[str], sink: list[str]) -> threading.Thread:
    """Echo a child's merged output to stderr while keeping a copy."""

    def pump(out: IO[str]) -> None:
        for line in out:
            sink.append(line)
            sys.stderr.write(line)
            sys.stderr.flush()

    assert proc.stdout is not None
    t = threading.Thread(target=pump, args=(proc.stdout,), daemon=True)
    t.start()
    return t


def run_make(*args: str) -> tuple[int, str]:
    log(f"make {' '.join(args)}")
    proc = subprocess.Popen(["make", "--no-print-directory", *args], cwd=REPO_ROOT, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    lines: list[str] = []
    t = stream(proc, lines)
    rc = proc.wait()
    t.join()
    return rc, "".join(lines)


def preflight(flink: sampler.FlinkRest) -> str | None:
    """Why the stack cannot be measured now, or None."""
    up = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True).stdout.split()
    missing = [c for c in RUNNING if c not in up]
    if missing:
        return f"not running: {', '.join(missing)}. Run make up"
    if flink.running_job() is None:
        return "the Flink enrichment job is not running. Run make flink-up"
    return None


SETTLE_S = 60.0
SETTLE_TIMEOUT_S = 300.0
SETTLED_LAG = 1_000


def settle(flink: sampler.FlinkRest) -> str | None:
    """Wait until the stack is steady: the Flink job has run every task for SETTLE_S with
    checkpoints completing and none failing or restoring, and no group lags more than
    SETTLED_LAG. Measuring a stack still recovering (after a laptop wake, a restart or
    a backlog) would report its recovery as the pipeline's numbers. None when steady."""
    deadline = time.monotonic() + SETTLE_TIMEOUT_S
    base: sampler.FlinkSample | None = None
    since = time.monotonic()
    why = "not checked yet"
    while time.monotonic() < deadline:
        jid = flink.running_job()
        cur = flink.checkpoints(jid) if jid and flink.all_tasks_running(jid) else None
        if cur is None:
            base, why = None, "the Flink job is not running every task"
        elif base is None or cur.job_id != base.job_id or any(
                cur.counts.get(k, 0) != base.counts.get(k, 0) for k in ("failed", "restored")):
            base, since, why = cur, time.monotonic(), "Flink checkpoints failed or the job restarted"
        else:
            lags = {g: gs.lag for g, gs in sampler.rpk_groups(list(sampler.STAGES)).items()}
            behind = {g: v for g, v in lags.items() if v > SETTLED_LAG}
            if behind:
                why = "lagging: " + ", ".join(f"{g} {v:,}" for g, v in behind.items())
            elif time.monotonic() - since >= SETTLE_S and cur.counts.get("completed", 0) > base.counts.get(
                    "completed", 0):
                return None
            else:
                why = f"waiting for {SETTLE_S:.0f} s of clean checkpoints"
        time.sleep(5.0)
    return f"the stack did not settle within {SETTLE_TIMEOUT_S:.0f} s: {why}"


def keep_awake() -> subprocess.Popen[bytes] | None:
    """macOS: hold off idle sleep while the report runs. A laptop that sleeps mid-run
    times out Flink's heartbeats and the report measures the wake-up instead."""
    if shutil.which("caffeinate") is None:
        return None
    return subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())])


def plan(csv: Path, rows: int) -> tuple[float, bool, str]:
    """(expected replay seconds at SPEED, inject?, why not)."""
    layout = read_layout(csv)
    span = event_span(csv, layout, rows)
    if span is None:
        raise SourceError(f"{csv}: no row has a timestamp")
    span_min = (span[1] - span[0]) / 60_000
    need = inject.load(ANOMALIES).required_min
    if span_min >= need:
        return span_min * 60 / SPEED, True, ""
    return span_min * 60 / SPEED, False, (
        f"{rows:,} rows cover {span_min:.1f} min of event time and the anomalies need {need:.1f} min "
        f"(about 650,000 rows)")


def replay(rows: int, injected: bool) -> tuple[subprocess.Popen[str], list[str], threading.Thread]:
    args = f"--limit {rows}" + (f" --inject {ANOMALIES.relative_to(REPO_ROOT)}" if injected else "")
    log(f"make replay SPEED={SPEED:g} ARGS=\"{args}\"")
    proc = subprocess.Popen(["make", "--no-print-directory", "replay", f"SPEED={SPEED:g}", f"ARGS={args}"],
                            cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    lines: list[str] = []
    return proc, lines, stream(proc, lines)


def run_id_from(lines: list[str]) -> str | None:
    for line in lines:
        if line.startswith("replayer: run "):
            return line.split()[2]
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="report", description=__doc__.splitlines()[0])
    ap.add_argument("--csv", type=Path, required=True, help="the AIS day's CSV (make passes AIS_CSV)")
    ap.add_argument("--rows", type=int, default=700_000)
    ap.add_argument("--chaos", action="store_true", help="kill the TaskManager, tracker and JobManager mid-run")
    ap.add_argument("--timeout", type=int, default=1800, help="seconds make reconcile-run may wait for the pipeline")
    ap.add_argument("--command", help="the command line to print at the top of the report")
    args = ap.parse_args(argv)
    command = args.command or f"make report ROWS={args.rows}" + (" CHAOS=1" if args.chaos else "")

    flink = sampler.FlinkRest(setting("FLINK_REST_URL", "http://localhost:8081"))
    if (why := preflight(flink)) is not None:
        print(f"report: {why}", file=sys.stderr)
        return 2
    try:
        expected_s, injected, inject_note = plan(args.csv, args.rows)
    except (OSError, SourceError, inject.InjectError) as e:
        print(f"report: {e}", file=sys.stderr)
        return 2
    if args.chaos and expected_s < MIN_CHAOS_REPLAY_S:
        print(f"report: CHAOS=1 needs a replay of at least {MIN_CHAOS_REPLAY_S:.0f} s at {SPEED:g}x to fit three "
              f"faults; {args.rows:,} rows take about {expected_s:.0f} s. Raise ROWS.", file=sys.stderr)
        return 2
    if not injected:
        log(f"no anomaly injection: {inject_note}")

    keep_awake()
    log("waiting for the stack to be steady")
    if (why := settle(flink)) is not None:
        print(f"report: {why}", file=sys.stderr)
        return 2

    env = environment.collect()
    log(f"commit {env.git_short}{' (uncommitted changes)' if env.git_dirty else ''}, zone package "
        f"{env.zone_package}, replay about {expected_s:.0f} s")
    smp = sampler.Sampler(flink)
    smp.start()

    proc, replay_lines, pump = replay(args.rows, injected)
    started = time.time()
    faults: list[Fault] = []
    chaos_error: str | None = None
    if args.chaos:
        try:
            chaos.run(flink, started, expected_s, lambda: proc.poll() is not None, faults)
        except (ChaosError, subprocess.SubprocessError) as e:
            chaos_error = str(e)
            log(f"fault sequence stopped: {e}")
    replay_rc = proc.wait()
    pump.join()
    run_id = run_id_from(replay_lines)
    if run_id is None:
        smp.stop()
        print(f"report: the replay printed no run id (exit code {replay_rc})", file=sys.stderr)
        return 1
    produced = json.loads((RUNS_DIR / run_id / "produced.json").read_text(encoding="utf-8"))

    reconcile_rc, reconcile_out = run_make("reconcile-run", f"RUN={run_id}", f"TIMEOUT={args.timeout}")
    score_rc: int | None = None
    score_out = ""
    if injected:
        score_rc, score_out = run_make("score", f"RUN={run_id}", "WAIT=600")

    smp.stop()
    smp.take()  # the final sample: everything has caught up
    samples = smp.samples()

    client = client_from_env()
    first_fault_ms = int(faults[0].killed_at * 1000) if faults else None
    rep = render.Report(
        command=command,
        env=env,
        run_id=run_id,
        rows=args.rows,
        speed=SPEED,
        produced=produced,
        injected=injected,
        inject_note=inject_note,
        faults=faults,
        chaos_error=chaos_error,
        throughput={g: sampler.throughput(samples, g) for g in sampler.STAGES},
        lag=sampler.lag_summary(samples),
        memory=sampler.memory_summary(samples),
        checkpoints=sampler.checkpoint_summary(samples),
        job_down=sampler.job_down(samples, [(f.killed_at, f.recovered_at or time.time()) for f in faults]),
        latencies=collect.latencies(client, run_id, first_fault_ms),
        reconciliation=collect.reconciliation(client, run_id),
        reconcile_rc=reconcile_rc,
        reconcile_output=reconcile_out,
        detection=collect.detection(client, RUNS_DIR / run_id, run_id) if injected else None,
        score_rc=score_rc,
        score_output=score_out,
        copies=collect.copies(client, run_id),
        finished_at=render.now(),
        sample_errors=dict(smp.errors),
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = render.report_path(OUT_DIR, env)
    path.write_text(render.render(rep), encoding="utf-8")
    render.update_index(INDEX, render.index_line(rep, path))
    log(f"wrote {path.relative_to(REPO_ROOT)} and the index in {INDEX.relative_to(REPO_ROOT)}")
    if rep.ok:
        log("OK")
        return 0
    log("FAIL: see the report")
    return 1


if __name__ == "__main__":
    sys.exit(main())
