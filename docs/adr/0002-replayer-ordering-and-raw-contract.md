# 0002. Replayer: event-time sort over raw lines, ais.raw v1 contract

Status: accepted (2026-11-02)

## Context

The replayer models a one-way sensor feed: it must send every CSV row, in event-time
order, keep enough to explain later rejects (the Flink enrichment job's DLQ), and
record exactly what was acknowledged for the zero-loss reconciliation job. A DMA day
is 3.2 GB and 18.9M rows.

## Decision

1. **polars scans the CSV as one string column per line.** The separator is `\x1f`
   and quoting is off. Only the timestamp is parsed in polars, to sort by
   `(event_ts, line_no)`; Python parses the fields per row. A malformed row can
   therefore never fail the scan or be dropped, and `raw` is the exact original line.
   The sort is deterministic, so two replays produce the same per-key order.
2. **Rows with an unparseable timestamp sort last** and are sent without pacing.
3. **The header is read, not assumed.** Names are normalised (`# Timestamp` →
   `timestamp`) and the timestamp format is detected from the first row. There is
   deliberately no month-first format, so `01/02` cannot be misread.
4. **Timestamps are epoch milliseconds (UTC) ints** in `event_ts` and
   `replay_send_ts`, not ISO strings. They are unambiguous, cheap to parse, and map
   directly to ClickHouse `DateTime64(3)`.
5. **`raw` is always present**, not only on rows that failed to parse. The Flink
   enrichment job decides validity, and the replayer cannot know which rows it will
   reject.
6. **produced.json counts broker acknowledgements**, not produce() calls. Offsets
   come from delivery callbacks.
7. **Every message carries `run_id`**, the same value as its `data/runs/<run_id>/`
   directory (`<UTC start>-<6 hex>`, e.g. `20261102T132701Z-6cdc0c`). Several runs
   can share `ais.raw`, so an offset range alone does not show which run a stored
   row came from. With `run_id`, reconciliation can check that every stored row in
   a run's offset range belongs to that run.

Measured on the pinned day (M-series laptop): full sort peak RSS about 1 GB, 19 s
before the first message. `--limit N` becomes a polars top-k, so smoke tests stay
cheap. Message building runs at about 73k rows/s; 100x needs about 22k/s.

## Alternatives

- **Typed `scan_csv` with all columns:** loses the original line, and ragged rows make
  the scan error or truncate rows.
- **Assume the file is already sorted (it is, for this day):** that is true for this
  day but not guaranteed for other days or for merged live captures. A heap-based
  bounded reorder would use less memory but is more code for no measured need.
- **Avro or Protobuf with Redpanda's schema registry:** worth revisiting when Flink
  owns `ais.enriched`. `schema_version` in every message leaves room for that.

## Consequences

- Messages are about 2x the CSV row size because of `raw`. lz4 recovers most of that on the wire.
- Re-running the replayer appends a second copy to `ais.raw`. Producer idempotence
  only removes retry duplicates *within* a run. Reconciliation is therefore scoped
  per run: for each partition, the offset range in `data/runs/<run_id>/produced.json`,
  checked against the `run_id` on the stored rows. Downstream dedup is the Flink
  enrichment job's dedup step (ADR 0015).
- Producing faster than the target rate is not throttled. Falling behind is not
  "repaid" later: the replayer bursts until it is back on schedule.
