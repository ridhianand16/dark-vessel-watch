# 0007. Injected anomalies: synthetic vessels in the replay, scored by exact alert_id

Status: accepted (2026-11-03). Amended by ADR 0008: the MMSI block moved to
200000000-200000999, because a leading 99 marks an aid to navigation, which the
tracker skips.

## Context

The spec asks for precision and recall on a set of injected, labelled anomalies, and
for a demo in which a ship is guaranteed to go dark. Real AIS has no ground truth: a
real gap may be a switch-off or a coverage hole.

Two facts about the data shape the design. Event time is dense: the DMA day has about
219 rows per second, so 200,000 rows cover only 16.7 minutes, and a 45-minute dark gap
needs about 54 minutes (650,000 rows). And an alert is identified by its event time
(ADR 0005), so the label can name the exact alert it expects.

## Decision

1. **Synthetic vessels go through the replayer** (`--inject anomalies.yaml`), not
   through a separate producer. Their rows are DMA-format CSV lines that take the same
   `build_message` path, are merged into the stream in event-time order, and are
   counted by reconciliation like any other row. Downstream they differ from real
   traffic only by MMSI.
2. **Reserved MMSI block 999000000-999000999.** MID 999 is not allocated, and the DMA
   day has no MMSI starting with 999. The replayer counts real rows in the block for
   every run; the scorer refuses a run where that count is not 0.
3. **Times are relative to the run's first event_ts**, so one file works for any day.
   The replayer computes the run's span first (one pass, about 4 s on the full file)
   and refuses, exit 2, if an event does not fit, naming the shortfall. A run that is
   too short would otherwise report a "miss" that was never observable.
4. **Vessels report until the end of the run.** A synthetic vessel that stopped early
   would go dark without being labelled. Real vessels stop with the data, and so does
   the watermark, so the end of the data raises nothing.
5. **Events are `dark` (silent for a while, reappears on its dead-reckoned track) and
   `jump` (position shifted in one report, stays shifted).** Each label carries the
   expected alert type, or null for a decoy that must not alert, and `trigger_ts`.
   The shipped file has three 45-minute gaps, three 30 km jumps, a moored vessel that
   goes silent (below the 2 kn rule) and a 0.5 km jump (below the 1 km floor).
6. **Scoring matches exact alert_ids.** The scorer derives each label's id with the
   tracker's own `alert_id()`. An alert at the wrong time is a false positive and a
   miss. Any other alert on a synthetic MMSI is a false positive. Alerts on real
   vessels are counted and shown, but not scored.
7. **Score only a finished run.** The scorer waits until the tracker has committed past
   the run's last `ais.raw` offsets and the alerts sink has caught up. A first version
   stopped as soon as the expected alerts appeared, and under-counted the alerts
   raised later in the run.

## Alternatives

- **Hand-written alert fixtures in tests only.** Already exist (`test_detect.py`). They
  do not exercise Kafka, the sinks or a restart.
- **Label real vessels by hand.** No ground truth for "switched off" versus "out of
  coverage", and slow.
- **Match by (type, mmsi) within a time window.** More forgiving, but it would hide an
  alert raised one report late, which is a real defect in event-time logic.
- **Lower `DARK_GAP_S` so a gap fits into 200k rows.** It would test a different rule
  from the one in the spec.

## Consequences

- **Verification runs need 700,000 rows** (58 min of event time), 75 s at 50x.
- **Precision is measured on decoys we chose**, so it says that known non-events do not
  fire, not how the rules behave on real traffic. Real-traffic alerts need zones (M3)
  and a look at the map before they mean anything.
- **Synthetic tracks are simpler than real ones:** straight lines, constant speed,
  Gaussian noise of 5 m, a report every 10 s. They test the rules and the delivery
  path, not the filter's robustness to messy data.
- **The file is versioned** (`version: 1`) and its SHA-256 is recorded in each run's
  `labels.json`, so a score can be traced to the exact anomaly set.
