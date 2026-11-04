# 0005. Detections: in the tracker, on event time, one deterministic alert per event

Status: accepted (2026-11-03). Zone condition superseded by ADR 0019: DARK_GAP reads
the Flink job's zone tags, and the stub predicate described below is gone.

## Context

This adds two alert rules to the Faust tracker (`stream/faust_app`, ADR 0004):

- **DARK_GAP**: a vessel last moving faster than 2 kn has not been heard for more than
  30 min, and a zone predicate holds (near an EEZ boundary or protected area, not in
  port, not at the coverage edge; a stub returning true until the zones exist).
  The alert carries the Kalman-predicted position.
- **TELEPORT**: implied speed between consecutive fixes above 50 kn (ignoring pairs
  under 10 s or 1 km apart, which are GPS noise), or a Kalman innovation above a
  threshold. The outlier is not fed to the filter; after N rejections in a row the
  filter restarts.

Alerts go to `ais.alerts`. The spec requires that killing a worker mid-run creates no
duplicate alerts, and precision/recall on the injected anomalies needs the same input
to give the same alerts.

DARK_GAP is triggered by *absence*, so no record arrives to trigger it. Something has
to look at every vessel periodically and ask "how long since we heard from you?",
measured against a clock. The original plan was a Faust timer every few seconds that
scans the table against a per-partition watermark (max `event_ts` seen) and sets an
"already alerted" flag on the vessel.

Checked in faust-streaming 0.15.4, three things rule out the timer:

1. **A timer cannot write the table.** `Table.partition_for_key` raises
   `TypeError("Cannot modify table key from outside of stream iteration")` when there
   is no current event, so the flag cannot be set from a timer.
2. **It would race with the agent.** The agent reads a vessel's state, awaits the
   broker's acknowledgements (ADR 0004), then writes the new state. A timer running in
   between could set the flag on the old state, which the agent then overwrites, or
   the other way round.
3. **It has nothing new to see.** The watermark moves only when records arrive, that
   is, inside the agent. Between batches a timer sees the same watermark the last
   batch left.

## Decision

1. **Both rules run in the tracker's agent, on the same delivery path as tracks.**
   Alerts from a batch are sent and acknowledged before the batch's states are
   written (ADR 0004's outputs-before-state).
2. **DARK_GAP is scanned at the end of a batch, gated by event time.** The watermark
   is the max `event_ts` per **(partition, run_id)**. When a partition's watermark
   enters a new `DARK_SCAN_STEP_S` step (60 s of event time by default), the agent
   scans the stored vessels of that partition. It sends their DARK_GAP alerts, waits
   for the acks, and only then writes `gap_alerted = true`. Only partitions present in
   the current batch are scanned: Faust's RocksDB iteration already skips standby
   replicas, and every partition whose watermark moved is in the batch by definition.
   It also provides the event the table write needs (`as_current_event`).
3. **The watermark is per run, not just per partition.** Each replay restarts event
   time. With a partition-only watermark, a second replay of the same day would sit a
   whole day behind the first run's watermark, and every vessel in it would look dark
   at once.
4. **The vessel's return also raises DARK_GAP.** When a fix arrives more than 30 min
   after the vessel was last heard and the gap was not yet alerted, `apply()` raises
   it. The scan only runs once per step and never runs for a partition that has gone
   quiet, so without this a gap between 30 and 31 minutes, or one at the end of a
   replay, could be missed depending on when scans happened to run.
5. **Alert content is a pure function of state.** DARK_GAP's `ts` is
   `last_heard + 30 min`, the event time at which the rule became true. Its predicted
   position is the filter's prediction at that time. It does not depend on when the
   scan ran or on the watermark, so the scan and the return path produce
   byte-identical messages for the same gap. TELEPORT's `ts` is the offending fix's
   time. No field uses the wall clock.
6. **`alert_id = UUIDv5(type | mmsi | run_id | trigger_ts)`** with a fixed namespace.
   `trigger_ts` is the offending fix (TELEPORT) or the last fix heard before the gap
   (DARK_GAP), so one gap has one id however often it is detected. The alerts sink will
   dedup on it (ReplacingMergeTree key), as the other sinks do on
   `(src_partition, src_offset)`.
7. **"Heard" is separate from "accepted".** The state keeps the last accepted fix (the
   filter's clock, and the reference TELEPORT measures jumps from) and the last fix
   heard (accepted or rejected). A rejected fix still ends a silence: a vessel that is
   transmitting is not dark, even if what it sends is implausible. Exact repeats of the
   last fix heard are duplicates. Without that, base-station copies of one outlier
   (about 40% of DMA rows are copies) would count as further rejections and restart
   the filter on a single bad fix.
8. **One TELEPORT per jump.** Only the first rejection in a row raises an alert. The
   `TELEPORT_MAX_REJECTS`th (default 3) restarts the filter at that fix without an
   alert: the vessel really is over there. `fix_seq` keeps counting.
9. **Thresholds live in `config.DetectionConfig`** only, overridable from the
   environment, and validated at startup.
10. **Vessel state goes to version 2.** v1 states are upgraded on read: their partition
    is unknown, so the scan skips them until the vessel reports again. The return path
    still catches their gaps.

TELEPORT's innovation rule needs both NIS > 50 and an innovation of at least 1 km.
NIS alone flags a 200 m wobble when the filter is confident. Distance alone ignores
how uncertain the prediction is after a long silence.

## Alternatives

- **Faust timer + table writes via a fake current event.** Works around (1), but
  not (2), and the timer still only sees batch-boundary watermarks (3).
- **Timer for the scan, flag set by the agent later.** The timer would hand pending
  flags to the agent through memory. A crash loses them, which is harmless given
  deterministic ids, but it adds a second writer's worth of ordering rules for
  nothing.
- **Wall-clock scan interval.** Simpler to explain, but at 50x replay 5 s of wall
  time is 4 min of event time, and at 1x it scans 12 times a minute for nothing. A
  step of event time keeps scan cost proportional to data and detection delay
  constant in event time.
- **Partition-only watermark.** Rejected by (3) above.
- **Alert `ts` = scan watermark.** Matches "when it fired" literally, but makes the
  bytes depend on scan timing and restart history, so reprocessing would not give
  identical alerts.
- **Per-record scan (no step).** Measured below: too expensive at full replay speed.

## Consequences

- **Scan cost.** Parsing 10,000 stored states and skipping them all takes about
  54 ms. Full decode of every state took 92 ms, so the scan checks the parsed JSON
  before building numpy arrays. At 50x replay a 60 s step is about one scan per
  1.2 s. The scan blocks the event loop while it runs, which adds to the latency of
  that batch. If the vessel count grows by an order of magnitude, keep an in-memory
  deadline index per partition instead.
- **Detection delay** for a still-silent vessel is at most one step (60 s of event
  time) past the 30 min threshold.
- **End of a run.** The last part of a step after the final scan is never scanned:
  a vessel whose 30 min expire in that window, and that never reports again, gets no
  alert. That is at most 60 s of event time at the very end of a replay.
- **A bogus future `event_ts`** moves its partition's watermark ahead and makes every
  moving vessel on that partition look dark. The raw stream is not validated yet. The
  Flink enrichment job must drop such records before Faust reads `ais.enriched`.
- **Rolling back to v1 code is not possible** once v2 states are in the changelog:
  the v1 tracker rejects them. Rolling back means `make nuke` or deleting the tracker
  changelog and local state.
- **Still one worker.** The scan is per assigned partition, so it is rebalance-safe in
  principle. It needs the same multi-worker testing as ADR 0004.
- **The zone predicate is called only for vessels that already passed the cheap
  checks**, so a real zone lookup behind it runs for a handful of vessels per scan,
  not all of them.
