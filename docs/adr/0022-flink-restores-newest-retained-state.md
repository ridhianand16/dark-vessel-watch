# 0022. The Flink job always starts from its newest retained state

Status: accepted (2026-11-04)

## Context

The enrichment job runs on a Flink session cluster in Compose, without high availability.
A JobManager holds the running job in memory only, so a JobManager restart (a crash, an
out-of-memory kill, `docker restart`) drops the job. Its checkpoints survive on the
`flink-data` volume: they are retained on cancellation, three at a time (ADR 0014).

Before this decision, `flinkctl.sh submit` (used by `make flink-up` and `make up`)
started the job from the Kafka group's committed offsets whenever no savepoint was
named, and `restart` restored the newest savepoint even when newer checkpoints existed.
Neither is safe after losing the JobManager:

- **Committed offsets are not the job's state.** The source commits offsets after a
  checkpoint completes, and the sinks commit that checkpoint's Kafka transactions in a
  separate callback. A restart between the two leaves offsets past records whose output
  was never committed; the new job's sinks abort those transactions, and the records are
  lost. A fresh job also loses the dedup keys (repeats in the next 2 minutes of event
  time pass as new) and any records held in the zone buffer.
- **An older savepoint repeats output.** Restoring a savepoint taken before newer
  checkpoints rewinds the source, and every record since is written to `ais.enriched`
  again: the reconciliation job's `flink_balance` check then fails with offsets twice.

## Decision

Every start restores the newest completed state on the volume: the most recent
`_metadata` among `/flink-data/savepoints/*/` and `/flink-data/checkpoints/*/chk-*/`. A
checkpoint writes `_metadata` last, when it completes, so an unfinished one is never
picked. The job starts fresh, from committed offsets or the start of `ais.raw`, only when
the volume holds neither, which is the case on a fresh clone or after `make nuke`.
`SAVEPOINT=<dir>` still picks a state explicitly. The script logs which one it used.

## Alternatives

- **JobManager high availability.** Flink's HA services need ZooKeeper or Kubernetes.
  ZooKeeper is another 256 MB or more on a laptop that has no headroom, for a single
  JobManager that Docker restarts anyway. On Kubernetes this becomes the right answer.
- **Keep starting from committed offsets.** Simple, but loses data in the window above
  and gives up the dedup state, which is the opposite of what the job is for.
- **Restore by hand** (what ADR 0014 said). Correct when done right, but the default
  command did the wrong thing, and nothing told the operator.

## Consequences

- After a JobManager restart the job stays down until `make flink-up` (or `make up`), and
  then resumes exactly where its last checkpoint left off. The tracker gets no input in
  between. This is listed under Known issues.
- A state the current job cannot restore (an operator removed from the graph) fails the
  submit loudly. `--allowNonRestoredState` is never used: silently dropping state is what
  this decision avoids. To start over deliberately, `make nuke`, or move the
  `flink-data` volume's contents aside.
- `make flink-restart` keeps its meaning: it stops with a savepoint, which is then the
  newest state.
