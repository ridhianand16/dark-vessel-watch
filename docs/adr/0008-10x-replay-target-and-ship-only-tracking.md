# 0008. Replay at 10x on one worker per service; track only ship stations

Status: accepted (2026-11-03)

## Context

The project runs on one 8 GB Apple M1 laptop. Redpanda, ClickHouse, PostGIS, two
ClickHouse sinks and the tracker already share a Docker VM of 3.9 GB, and Flink is
still to come.

The DMA day has about 219 rows per second of event time. At 50x that is about 11,000
rows/s into `ais.raw`. One tracker worker reads about 4,200 records/s (measured on a
529,812-message backlog), so 50x would need at least three workers, and the tracker
would fall behind on one. At 10x the input is about 2,200 rows/s, about half of what one
worker can take.

The tracker also spent work on stations that are not ships. ITU-R M.585 encodes the
station type in the MMSI's leading digits: coast stations (00), aids to navigation (99),
SAR aircraft (111) and distress beacons (970/972/974) report positions over AIS too.
They do not move like ships, and their silences are not "going dark". In a 10,000-row
replay, 910 records (9.1%) came from non-ship stations: 682 coast and 228 aid to
navigation.

## Decision

Replay at 10x is the target, with one worker per service and 6 partitions, because the
project runs on an 8 GB laptop. Non-ship stations (identified by MMSI range) are stored
in full but skipped by the tracker, with a counted reason. Faster replay is out of
scope; docs/architecture.md explains how to get there (more workers up to the partition count,
dropping numpy from the Kalman step, filtering earlier in Flink).

In practice:

- `make replay` defaults to `--speed 10`; `SPEED=` or a `--speed` in `ARGS` overrides it.
- `stream/faust_app/mmsi.py` maps an MMSI to a station class. First digit 2-7 and the
  prefix 98 (craft associated with a parent ship) are ships. Everything else, including
  values outside nine digits, is not.
- The tracker's `decode()` returns `Skip.NOT_A_SHIP` right after the MMSI is validated.
  Such a record never reaches vessel state, `ais.tracks` or `ais.alerts`. It is counted
  as `not_a_ship`, and the per-batch `read` counter lets the log show that the skip
  and result counters add up to the records consumed.
- `ais.raw`, the raw sink and reconciliation are unchanged.

## Alternatives

- **50x with more workers.** `ais.raw` has 6 partitions, so up to 6 Faust workers
  could share the load. Each worker is another process with its own RocksDB, about
  150 MiB at rest with a 512 MiB limit. Three or more workers would take 0.5 to 1.5 GB
  that the laptop does not have once Flink runs. It would also turn a single-worker
  demo into a rebalancing demo, which the spec does not ask for.
- **Filter non-ships in the replayer.** It is the cheapest place to drop them, but the
  raw store would lose them, against the spec's "stored in full". The replayer models a
  diode-only sensor feed and should not judge content: produced counts would no longer
  match what the sensor sent, and the live source would need the same filter. Filtering
  belongs in processing. The Flink validation job is the natural place later, since
  it sits ahead of every consumer.

## Consequences

- **The latency target is p99 under 2 s at 10x.** A full day takes about 2.4 hours to
  replay at that speed, so measured runs use slices of the day.
- **Faster replay is documented, not built.** docs/architecture.md lists the levers
  in order of cost.
- **A ship that transmits under a non-ship MMSI is invisible to detection.** A vessel
  faking an aid-to-navigation MMSI would raise no DARK_GAP or TELEPORT. Its rows are
  still in the raw store, and the `not_a_ship` counter shows the volume. Detecting
  ship-like motion under a non-ship MMSI is a candidate rule, not built.
- **The injected vessels moved to 200000000-200000999.** The old block started with 99,
  which now reads as an aid to navigation. MID 200 is unallocated and absent from the
  2026-10-28 day. ADR 0007 carries the amendment.
- **Group-call MMSIs (0 followed by a non-zero digit) and handhelds (8) are skipped.**
  Neither is a ship's own identity under M.585. If a real dataset uses them for
  vessels, the table in `mmsi.py` is the one place to change.
