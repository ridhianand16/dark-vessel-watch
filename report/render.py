"""Render a results report as Markdown, and keep the index in docs/results.md."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from report.chaos import Fault
from report.collect import Copies, Detection, Latencies, Quantiles, Reconciliation
from report.environment import Environment
from report.sampler import STAGES, Checkpoints, Memory, Throughput

INDEX_START = "<!-- reports:start -->"
INDEX_END = "<!-- reports:end -->"
_CHK = re.compile(r"/chk-(\d+)/?$")


@dataclass
class Report:
    command: str
    env: Environment
    run_id: str
    rows: int
    speed: float
    produced: dict[str, Any]
    injected: bool
    inject_note: str
    faults: list[Fault]
    chaos_error: str | None
    throughput: dict[str, Throughput | None]
    lag: dict[str, tuple[int, int]]
    memory: Memory
    checkpoints: Checkpoints
    latencies: Latencies
    reconciliation: Reconciliation | None
    reconcile_rc: int
    reconcile_output: str
    detection: Detection | None
    score_rc: int | None
    score_output: str
    copies: list[Copies]
    finished_at: datetime
    sample_errors: dict[str, int] = field(default_factory=dict)
    job_down: list[float] = field(default_factory=list)  # samples with no running job outside the faults

    @property
    def ok(self) -> bool:
        return (self.produced.get("status") == "complete" and self.reconcile_rc == 0 and self.chaos_error is None
                and not self.job_down
                and self.score_rc in (None, 0) and all(c.duplicates == 0 for c in self.copies))


# ------------------------------------------------------------------ format


def n(v: int) -> str:
    return f"{v:,}"


def ms(v: float) -> str:
    return f"{v:,.0f} ms" if v < 10_000 else f"{v / 1000:,.1f} s"


def mib(v: int) -> str:
    return f"{v / 1024**2:,.0f} MiB"


def ratio(num: int, den: int) -> str:
    return f"{num / den:.2f}" if den else "-"


def table(head: list[str], rows: list[list[str]]) -> list[str]:
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in head) + "|"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return out


def state(path: str | None) -> str:
    """'checkpoint 123 of job <id>' or 'savepoint ...' from a restored state's path."""
    if not path:
        return "nothing (the job started fresh)"
    clean = path.removeprefix("file:")
    m = _CHK.search(clean)
    if "/savepoints/" in clean:
        return f"savepoint `{clean}`"
    if m:
        job = clean.rstrip("/").split("/")[-2]
        return f"checkpoint {m.group(1)} of job `{job}`"
    return f"`{clean}`"


def is_checkpoint(path: str | None) -> bool:
    return bool(path) and "/savepoints/" not in str(path) and _CHK.search(str(path).removeprefix("file:")) is not None


def report_path(out_dir: Path, env: Environment) -> Path:
    """docs/results/<date>-<short sha>.md, with -2, -3 ... when one exists already."""
    stem = f"{env.started_at:%Y-%m-%d}-{env.git_short}"
    path = out_dir / f"{stem}.md"
    i = 2
    while path.exists():
        path = out_dir / f"{stem}-{i}.md"
        i += 1
    return path


# ------------------------------------------------------------------ sections


def summary(r: Report) -> list[str]:
    p = r.produced
    injected = (f" plus {n(r.detection.rows_injected)} labelled synthetic rows" if r.detection else "")
    parts = [f"Replayed {n(r.rows)} rows of the {r.env.ais_day} DMA day{injected} at {r.speed:g}x "
             f"(run `{r.run_id}`, {n(int(p.get('sent', 0)))} sent in {float(p.get('elapsed_s', 0)):.0f} s)."]
    if r.faults:
        parts.append("During the replay the Flink TaskManager, the tracker and the Flink JobManager were "
                     "killed one after the other, and each was recovered before the next.")
    rec = r.reconciliation
    if rec is None:
        parts.append(f"The reconciliation job wrote no verdict (exit code {r.reconcile_rc}).")
    elif rec.failed:
        parts.append(f"Reconciliation FAILED: {len(rec.failed)} checks failed "
                     f"({', '.join(sorted({c.check for c in rec.failed}))}).")
    else:
        parts.append(f"Reconciliation found no loss: every check passed on all "
                     f"{len({c.partition for c in rec.checks if c.partition >= 0})} partitions"
                     f"{' despite the three kills' if r.faults else ''}.")
    jm = next((f for f in r.faults if f.name == "jobmanager"), None)
    if jm is not None:
        how = "restored" if is_checkpoint(jm.restored) else "did NOT restore a checkpoint; it restored"
        parts.append(f"After the JobManager kill, `make flink-up` {how} {state(jm.restored)}"
                     f"{' (the killed job)' if jm.job_before and jm.job_before in (jm.restored or '') else ''}.")
    if r.chaos_error:
        parts.append(f"The fault sequence did not complete: {r.chaos_error}.")
    if r.job_down:
        parts.append(f"**The Flink job was also down outside the injected faults** ({len(r.job_down)} samples, "
                     f"first at {datetime.fromtimestamp(r.job_down[0], UTC):%H:%M:%S} UTC), so latency and "
                     "throughput include an unplanned restart.")
    return [" ".join(parts)]


def environment(r: Report) -> list[str]:
    e = r.env
    rows = [
        ["Started", f"{e.started_at:%Y-%m-%d %H:%M:%S} UTC"],
        ["Finished", f"{r.finished_at:%Y-%m-%d %H:%M:%S} UTC"],
        ["Commit", f"`{e.git_sha}`" + (" with uncommitted changes" if e.git_dirty else "")],
        ["Machine", e.host],
        ["OS", e.host_os],
        ["Docker VM", e.docker],
        ["AIS day", f"{e.ais_day}, first {n(r.rows)} rows in event-time order"],
        ["Zone package", f"`{e.zone_package}`"],
        ["Replay", f"{r.speed:g}x, " + ("with injected anomalies" if r.injected else f"no injection: {r.inject_note}")],
    ]
    out = ["## Environment", "", *table(["", ""], rows), "", "Images of the running containers:", ""]
    out += table(["Container", "Image", "Image id"], [
        [i.container, f"`{i.tag}`", f"`{i.image_id}`" + ("" if i.current else " (the tag now points at a newer build)")]
        for i in e.images
    ])
    return out


def faults(r: Report) -> list[str]:
    if not r.faults and not r.chaos_error:
        return []
    t0 = datetime.fromisoformat(r.produced["started_at"]).timestamp() if "started_at" in r.produced else 0.0
    rows = []
    for f in r.faults:
        restored = "-" if f.name == "tracker" else state(f.restored)
        rows.append([
            {"taskmanager": "Flink TaskManager, SIGKILL",
             "tracker": "tracker, SIGKILL",
             "jobmanager": "Flink JobManager, SIGKILL"}[f.name],
            f"{f.killed_at - t0:.0f} s",
            {"taskmanager": "`docker start`, the job fails over by itself",
             "tracker": "`docker start`",
             "jobmanager": "`make flink-up`"}[f.name],
            "-" if f.outage_s is None else f"{f.outage_s:.0f} s",
            restored,
        ])
    out = ["## Faults", "",
           "Each fault started once the one before had recovered. Recovered means: for Flink, the job runs "
           "every task and has completed a checkpoint triggered after the restart; for the tracker, its group "
           "commits offsets again. The JobManager is killed rather than stopped: a graceful stop makes Flink "
           "delete the job's checkpoints when there is no high availability (ADR 0022, ADR 0023).", ""]
    out += table(["Fault", "Into the replay", "Recovery", "Until recovered", "Job restored"], rows)
    jm = next((f for f in r.faults if f.name == "jobmanager"), None)
    if jm and jm.output:
        lines = [ln for ln in jm.output.splitlines() if "flinkctl:" in ln]
        if lines:
            out += ["", "`make flink-up` after the JobManager kill:", "", "```", *lines, "```"]
    if r.chaos_error:
        out += ["", f"**The fault sequence stopped early:** {r.chaos_error}."]
    return out


def reconciliation(r: Report) -> list[str]:
    out = ["## Reconciliation", ""]
    rec = r.reconciliation
    if rec is None:
        return out + [f"No verdict in `dvw.reconciliation` (exit code {r.reconcile_rc}); see the output below."]
    t = rec.totals
    out += [f"From `make reconcile-run` (exit code {r.reconcile_rc}), read back from `dvw.reconciliation FINAL`. "
            f"{n(t.produced)} produced = {n(t.stored)} stored in `dvw.ais_raw`; "
            f"{n(t.enriched)} enriched + {n(t.dlq)} DLQ + {n(t.duplicates)} counted duplicates = "
            f"{n(t.enriched + t.dlq + t.duplicates)}; {n(t.tracks)} tracks = {n(t.rollup_fixes)} rollup fixes; "
            f"{n(t.untagged)} enriched records without zone tags.", ""]
    by: dict[str, list[str]] = {}
    for c in rec.checks:
        by.setdefault(c.check, []).append(c.status)
    rows = []
    for check, statuses in by.items():
        notes = sorted({c.note for c in rec.checks if c.check == check and c.status != "ok"})
        rows.append([f"`{check}`", str(statuses.count("ok")), str(statuses.count("warn")), str(statuses.count("fail")),
                     "; ".join(notes)[:300]])
    out += table(["Check", "OK", "Warn", "Fail", "Notes"], rows)
    return out


def latency_section(r: Report) -> list[str]:
    def row(name: str, q: Quantiles | None) -> list[str]:
        if q is None:
            return [name, "0", "-", "-", "-", "-"]
        return [name, n(q[0]), ms(q[1]), ms(q[2]), ms(q[3]), ms(q[4])]

    lat = r.latencies
    rows = [row("send to track stored in `dvw.tracks`, whole run", lat.track_stored)]
    if r.faults:
        rows.append(row("send to track stored, records sent before the first fault", lat.track_stored_before_faults))
    rows.append(row("send to message-triggered alert stored in `dvw.alerts`", lat.alert_stored))
    out = ["## Latency", "",
           "Exact percentiles over every record, from timestamps stored in ClickHouse: the replayer's "
           "`replay_send_ts` to the first `ingested_at` of the row. The target is p99 under 5 s at 10x.", ""]
    out += table(["Path", "n", "p50", "p95", "p99", "max"], rows)
    out += ["", "DARK_GAP alerts are left out of the alert row: they are raised by the absence of reports, and "
            "their trace record is the last report before the silence, sent at least the 30 min gap threshold "
            "(3 min at 10x) before the alert could exist. Their send-to-stored time measures the rule, not "
            "the pipeline."]
    if r.faults:
        out += ["", "The whole-run rows include records sent while a component was down, so their tail is the "
                "outage, not steady-state latency."]
    return out


def throughput_section(r: Report) -> list[str]:
    p = r.produced
    rows = [["replayer, CSV to ais.raw", n(int(p.get("sent", 0))), f"{float(p.get('elapsed_s', 0)):.0f} s",
             f"{int(p.get('sent', 0)) / max(float(p.get('elapsed_s', 1)), 1e-9):,.0f}", "-"]]
    for g, label in STAGES.items():
        t = r.throughput.get(g)
        rows.append([label, "-", "-", "-", "-"] if t is None else
                    [label, n(t.records), f"{t.seconds:.0f} s", f"{t.avg_per_s:,.0f}", f"{t.peak_per_s:,.0f}"])
    out = ["## Throughput", "",
           "Records each consumer group committed while it was moving, from its committed offsets sampled "
           "every 5 s (so each span is good to about 5 s). At 10x every stage is paced by the replay; the peak "
           "30 s rate, reached while catching up after a fault, is closer to what a stage can do. Records are "
           "offsets, so on `ais.enriched`, which Flink writes in transactions, they include one commit marker "
           "per transaction and partition.", ""]
    out += table(["Stage", "Records", "Active", "Average per s", "Peak 30 s, per s"], rows)
    return out


def lag_section(r: Report) -> list[str]:
    rows = [[label, n(r.lag[g][0]), n(r.lag[g][1])] for g, label in STAGES.items() if g in r.lag]
    out = ["## Consumer lag", "",
           "From `rpk group describe` every 5 s, summed over the group's partitions. Final is the last sample, "
           "taken after the reconciliation job saw every group past the run. The Flink job commits offsets "
           "only after each checkpoint (every 3 s). Flink writes `ais.enriched` in transactions, and each "
           "commit marker takes an offset the tracker never reads, so a caught-up tracker can still show a "
           "small lag.", ""]
    return out + table(["Group", "Max", "Final"], rows)


def checkpoints_section(r: Report) -> list[str]:
    c = r.checkpoints
    rows = [
        ["Completed", n(c.completed)],
        ["Failed", n(c.failed)],
        ["Restores (every restart attempt counts)", n(c.restores)],
        ["Average duration", "-" if c.avg_ms is None else ms(c.avg_ms)],
        ["Max duration", "-" if c.max_ms is None else ms(c.max_ms)],
        ["Job instances", ", ".join(f"`{j}`" for j in c.jobs) or "-"],
    ]
    out = ["## Flink checkpoints", "",
           "From the Flink REST API (`/jobs/<id>/checkpoints`) every 5 s. Counts are the change in the job's "
           f"counters over the run, per job instance. Durations are end to end, over the {n(c.sampled)} "
           "completed checkpoints seen in the sampled history (the API keeps the last 10). A checkpoint in "
           "flight when the JobManager was killed is in neither.", ""]
    return out + table(["", ""], rows)


def memory_section(r: Report) -> list[str]:
    m = r.memory
    rows = [[name, mib(m.peak[name]), mib(m.limit[name]), f"{m.peak[name] / m.limit[name]:.0%}" if m.limit[name] else "-"]
            for name in sorted(m.peak, key=lambda k: -m.peak[k])]
    out = ["## Memory", "",
           f"From `docker stats` every 5 s. Peak total {mib(m.peak_total)} (the highest sum in one sample, not "
           f"the sum of the peaks); {mib(m.final_total)} at the end.", ""]
    return out + table(["Container", "Peak", "Limit", "Peak of limit"], rows)


def detection_section(r: Report) -> list[str]:
    d = r.detection
    out = ["## Detection", ""]
    if d is None:
        return out + [f"Not scored: {r.inject_note or 'the run was not replayed with --inject'}."]
    out += [f"`make score` (exit code {r.score_rc}): {d.events} injected events ({d.decoys} decoys that must "
            f"not alert), {n(d.rows_injected)} synthetic rows. Matching is exact, on the alert_id derived from "
            f"type, MMSI, run and trigger time. Alerts on real vessels have no ground truth and are counted, "
            f"not scored. `dvw.alerts FINAL` holds {n(d.alert_rows)} rows for the run, "
            f"{n(d.alert_ids)} distinct alert_id.", ""]
    rows = [[t, str(d.tp[t]), str(d.fp[t]), str(d.fn[t]), ratio(d.tp[t], d.tp[t] + d.fp[t]),
             ratio(d.tp[t], d.tp[t] + d.fn[t]), n(d.real[t])] for t in d.tp]
    out += table(["Type", "TP", "FP", "FN", "Precision", "Recall", "Alerts on real vessels"], rows)
    out += [""] + [f"- {z}" for z in d.zones] + [f"- missed: {m}" for m in d.missed]
    out += [f"- unexpected: {u}" for u in d.unexpected]
    return out


def dlq_section(r: Report) -> list[str]:
    rec = r.reconciliation
    out = ["## DLQ by reason", ""]
    if rec is None or not rec.dlq_reasons:
        return out + ["No DLQ records for the run." if rec else "No reconciliation verdict to read the DLQ from."]
    total = sum(rec.dlq_reasons.values())
    rows = [[f"`{k}`", n(v), f"{v / rec.totals.produced:.2%}" if rec.totals.produced else "-"]
            for k, v in rec.dlq_reasons.items()]
    return out + table(["Reason", "Records", "Of produced"], rows + [["total", n(total), "-"]])


def duplicates_section(r: Report) -> list[str]:
    rows = [[f"`{c.table}`", f"`({c.identity})`", n(c.inserted), n(c.final), n(c.distinct), n(c.duplicates)]
            for c in r.copies]
    t = r.reconciliation.totals if r.reconciliation else None
    out = ["## Duplicates", "",
           "Per table, the run's rows as inserted (a sink that re-sends a batch after a crash adds copies until "
           "a merge removes them), with `FINAL`, and the distinct identities. Duplicates is `FINAL` minus "
           "distinct, and must be 0."
           + (f" Separately, the Flink job counted and dropped {n(t.duplicates)} duplicate input reports "
              "(repeats in the source data, ADR 0015)." if t else ""), ""]
    return out + table(["Table", "Identity", "Inserted", "FINAL", "Distinct", "Duplicates"], rows)


def raw_output(r: Report) -> list[str]:
    out = ["## Command output", "", "<details><summary><code>make reconcile-run</code></summary>", "", "```",
           r.reconcile_output.strip(), "```", "", "</details>"]
    if r.score_output:
        out += ["", "<details><summary><code>make score</code></summary>", "", "```", r.score_output.strip(), "```",
                "", "</details>"]
    return out


def render(r: Report) -> str:
    title = f"# Results report, {r.env.started_at:%Y-%m-%d}, `{r.env.git_short}`"
    lines = [title, "", "```sh", r.command, "```", "", *summary(r)]
    for section in (environment, faults, reconciliation, latency_section, throughput_section, lag_section,
                    checkpoints_section, memory_section, detection_section, dlq_section, duplicates_section,
                    raw_output):
        part = section(r)
        if part:
            lines += ["", *part]
    errs = {k: v for k, v in r.sample_errors.items() if v}
    if errs:
        lines += ["", "Samples that could not be taken (a source was down): "
                  + ", ".join(f"{k} {v}" for k, v in errs.items()) + "."]
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------- index


def index_line(r: Report, path: Path) -> str:
    verdict = "reconciliation OK" if r.reconciliation and not r.reconciliation.failed and r.reconcile_rc == 0 \
        else "reconciliation FAILED"
    chaos = ", three components killed" if r.faults else ""
    return (f"- [{r.env.started_at:%Y-%m-%d}, `{r.env.git_short}`](results/{path.name}): "
            f"{n(r.rows)} rows at {r.speed:g}x{chaos}, {verdict}")


def update_index(index: Path, line: str) -> None:
    """Put `line` first between the index markers in docs/results.md."""
    text = index.read_text(encoding="utf-8")
    if INDEX_START not in text or INDEX_END not in text:
        raise ValueError(f"{index} has no {INDEX_START} ... {INDEX_END} block")
    head, rest = text.split(INDEX_START, 1)
    body, tail = rest.split(INDEX_END, 1)
    entries = [ln for ln in body.strip().splitlines() if ln.startswith("- ")]
    index.write_text(f"{head}{INDEX_START}\n" + "\n".join([line, *entries]) + f"\n{INDEX_END}{tail}", encoding="utf-8")


def now() -> datetime:
    return datetime.now(UTC)
