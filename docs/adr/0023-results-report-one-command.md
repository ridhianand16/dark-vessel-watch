# 0023. One command writes the results report, with faults injected on request

Status: accepted (2026-11-04)

## Context

The numbers in `docs/results.md` were taken by hand, one command at a time, over
several days and stack states. They cannot be compared run to run, and a reader cannot
tell which commit, images or zone package produced them. The project has no monitoring
stack by design, so the proof has to be a document generated from the run itself.

Fault injection needs a choice about the JobManager. A graceful stop (`docker stop`)
suspends the job, and without high availability Flink then deletes its checkpoints
(ADR 0022). `make flink-up` would then start the job fresh from committed offsets, which
loses the dedup state and does not test recovery from a checkpoint.

## Decision

`make report [ROWS=700000] [CHAOS=1]` runs one replay at 10x and writes
`docs/results/<date>-<short sha>.md`, with the exact command at the top, and adds a line
to the index in `docs/results.md`. It reuses the existing tools rather than measuring
twice: `make replay`, `make reconcile-run` (its verdict is read back from
`dvw.reconciliation`), `make score`, and the latency queries of `reconcile/latency.py`.

- **Sampling.** A thread samples every 5 s: committed offsets and lag per consumer group
  (`rpk group describe`), memory per container (`docker stats`), and the Flink job's
  checkpoint counters and recent history (REST API). Throughput per stage comes from the
  committed offsets. Latency does not: it is exact, from timestamps stored in ClickHouse.
- **Default size.** 700,000 rows, because the labelled anomalies need 54 min of event
  time (about 650,000 rows). A smaller `ROWS` replays without them, and the report says
  detection was not scored.
- **Faults.** With `CHAOS=1`, at 20%, 45% and 70% of the expected replay time, each
  after the one before has recovered: SIGKILL the TaskManager (then `docker start`),
  SIGKILL the tracker (then `docker start`), SIGKILL the JobManager (then `make
  flink-up`). The JobManager is killed, not stopped, so its checkpoints survive and
  `make flink-up` restores the newest one. The report reads the restored path from the
  REST API. It does not trust `is_savepoint`, which Flink sets for any state restored
  with `-s`.
- **Latency.** Send to track stored, and send to stored for alerts raised by a message.
  DARK_GAP is left out because it is raised by silence. With faults, the report also
  gives track latency for records sent before the first fault, the steady state.
- **Steady start.** The report replays only once the Flink job has run for 60 s with no
  failed checkpoint or restore and every group lags less than 1,000 records, and keeps a
  Mac awake with `caffeinate`. A first full run, started as the laptop woke from sleep,
  saw the TaskManager's heartbeat time out twice before any fault and measured the
  recovery instead of the pipeline. Samples where the job is down outside an injected
  fault are reported as an unplanned restart, and the run counts as failed.
- A report is written whatever the outcome; a failed check is in it, and the command
  exits 1.

## Alternatives

- **Prometheus and Grafana in Compose.** They cost memory the 8 GB laptop does not have.
  A dashboard is also not a committed, reviewable artefact. They belong to the deploy
  kit, off by default.
- **Stop the JobManager gracefully.** This is closer to a planned restart, but it tests
  a fresh start, not checkpoint recovery. `make flink-savepoint` covers planned restarts.
- **Throughput from ClickHouse `ingested_at`.** It would be exact for the sinks, but
  there is no stored timestamp for the Flink job's or the tracker's progress. Committed
  offsets measure every stage the same way.

## Consequences

- Every headline number in the README comes from one report, so it has a commit, a date
  and a command.
- A CHAOS=1 report's whole-run latency is dominated by the outages, by design. The
  steady-state row is the one to compare against the 5 s target.
- Checkpoint durations come from the history the REST API keeps (the last 10), sampled
  every 5 s, so a burst of more than 10 checkpoints between samples would be undercounted.
  The counts come from the job's counters and are complete, except a checkpoint in flight
  when the JobManager was killed.
- A full CHAOS=1 report takes about 10 to 15 minutes.
