# 0018. Flink zone tagging: zones read at startup, one shared index, a buffer for the race

Status: accepted (2026-11-04)

## Context

The Flink enrichment job tags every valid AIS record with `eez_id`, `mpa_ids`,
`in_port`, `dist_eez_boundary_m`, `dist_mpa_m` and `zones_version`. DARK_GAP needs
these to tell a vessel going dark near a boundary or protected area from one going dark
in port.

The zones are the active package in PostGIS, already reprojected to EPSG:25832 and cut
into pieces of at most 256 vertices by the loader (ADR 0013, ADR 0017). Packages change
rarely: a few times a year, when a source is updated. The job runs 4 tagging tasks in
one TaskManager with about 520 MB of heap.

Four questions:

1. How do the zones get into the job, and how does a new package get in?
2. Where does the in-memory index live?
3. What happens to AIS records that arrive before the zones?
4. What if the job starts before any package is loaded?

## Decision

**A source that reads the active package at startup.** A parallelism-1 Flink source
(`ZoneSource`) reads every piece of the active package over JDBC, then emits a "zones
complete" marker carrying the piece count. The pieces and the marker are broadcast to
every tagging task. The tagging operator is a `KeyedBroadcastProcessFunction`: its AIS
input is keyed by MMSI, its zone input is broadcast.

The read is one REPEATABLE READ transaction that takes ACCESS SHARE locks on
`refdata_packages` and both piece tables before its first query. A loader swap either
committed before the read (and is fully visible) or waits for it, so the version and
the pieces always come from the same load. Each row's `package_version` is still
checked against the active version. The read takes 2.7 s on an idle stack and 12.6 s
with a Kafka backlog competing for the CPU.

**A new package means a restart.** Stop with a savepoint, `make refdata-load`, start from
the savepoint (`make flink-restart`). The source re-reads on every start, including
starts from a savepoint, so the restarted job picks up whatever package is active. Every
record carries `zones_version`, so it is always clear which package tagged it. Restarts
after a failure also re-read, so a package loaded while the job was running takes effect
at the next failover, not at a moment chosen by an operator. That is acceptable for
reference data that changes a few times a year, and `zones_version` shows when it
happened.

To re-read after every restore, the source is declared unbounded and never finishes.
After the marker it idles and emits a final `Long.MAX_VALUE` watermark, so the zone
input never holds back event time. A bounded source would finish, and Flink records a
finished source in its checkpoints and does not run it again on restore (FLIP-147). The
restored tagging tasks would then wait for zones that never come. The reader keeps its
one split in every checkpoint, so a restored reader reads again.

**One index per TaskManager, outside Flink state.** The index is three JTS STRtrees over
the pieces: areas for containment, protected areas and boundary lines for nearest-point
search. Measured on the real package (34,706 pieces, 3.1 million vertices): 65 MB of
heap, with coordinates packed as plain doubles. Building it needs 115 MB at the peak,
while the WKB is still held.

- Each of the 4 tasks receives every piece, but they share one index: the first task to
  reach the marker builds it (580 to 950 ms), and the others wait for it and get the
  same immutable object (`SharedZones`). Four private copies would take 260 MB of the
  520 MB heap.
- The geometry is not in broadcast state. Every task snapshots its broadcast state in
  full at every checkpoint: 4 × 55 MB every 3 s. The broadcast state holds only the
  last package version, so a restore that picks up a newer package logs the change.
- Tagging comes after dedup, so duplicates are never tagged.

Per record, the point is projected to EPSG:25832 with proj4j (within 1 cm of PostGIS
across the area, tested at both edges). Containment counts the boundary as inside, like
`ST_Intersects`. EEZs overlap only in slivers of a few square metres at shared
boundaries, and there the smallest `zone_id` wins. Distances follow ADR 0013: find the
nearest piece and the nearest point on it in UTM, then report the geodesic distance to
that point on the WGS84 ellipsoid (GeographicLib). For protected areas this is done only
when the point is outside all of them, so the nearest point is on a real edge, never on
a cut line (ADR 0017). A lookup averages 36 µs.

**A buffer for the race between the two inputs.** Even with the package in PostGIS
before the job starts, the zone source and the Kafka source start together, and the
tagging operator processes whichever input has data. A Kafka backlog arrives in
milliseconds; the zones take seconds. Until the marker arrives, each AIS record is
appended to a per-MMSI list in keyed state. On the marker, every list is emitted in
order and cleared. No record is ever tagged "no zone" because the zones were still
loading, and per-vessel order, which the tracker relies on, is kept. The buffer is Flink
state, so a checkpoint taken while it holds records keeps them, alongside source offsets
that are already past them. Measured when started on a 300,000-record backlog: 58,344
records buffered and flushed. The reconciliation check now also fails on any enriched
record without tags.

**A startup gate.** The job's `main`, which runs in the `flink-submit` container, polls
PostGIS every 5 s until `refdata_packages` has an active row, and only then builds and
submits the job. Without a package, `make flink-up` waits and prints how to load one.

## Alternatives

- **A compacted `ais.zones` topic**, published from PostGIS after each load and read as
  an ordinary broadcast stream. New packages would go live without a restart. But it
  adds a topic, a publisher and a second copy of the package to keep consistent with
  PostGIS. The job would also have to switch indexes in the middle of the stream: which
  records were tagged with which version would depend on how the two streams
  interleaved, and a replay would not reproduce it. A restart per package is simpler,
  and it is what the change frequency calls for.
- **Load the zones in `open()`** of the tagging function, with no source and no buffer.
  This is simpler, but each of the 4 tasks would query PostGIS at every start. A task
  could also start at a different moment from its siblings and see a different active
  package. The broadcast gives every task the same read.
- **Hold the AIS source back until the zones are in**, instead of buffering: through
  Flink's internal input selection on a custom two-input operator. Records would stay
  in Kafka rather than the heap. But it is an internal API, it interacts with checkpoint
  barrier alignment, and the buffer it replaces is bounded by read rate × zone load time
  (tens of MB, measured above).
- **Keep the zones in broadcast state** and do not re-read on restore. Correct, but each
  checkpoint would write 4 × 55 MB, against a 3 s checkpoint interval on a laptop disk.
- **One index per task**, as first planned. It does not fit: 4 × 65 MB.

## Consequences

- PostGIS must be reachable whenever the job starts or restarts, including after a
  failure. If it is not, the zone source fails and the restart strategy retries every
  5 s. The startup gate only covers the first submission.
- The POSTGRES_* settings, password included, are serialized into the job graph with the
  zone source. That is acceptable on a closed network. A secret mounted into the
  TaskManager would be the fix if it ever is not.
- After each restart the job writes nothing to `ais.enriched` for a few seconds, while
  the zones load, and then flushes the buffer in one burst. Latency for those records
  includes the zone load.
- The buffer's size is read rate × zone load time. A cold start on a full day of backlog
  could buffer a few hundred thousand records in the heap. Starting from a savepoint, as
  `make flink-restart` does, only meets the backlog from the downtime.
- Records flushed from the buffer leave without a Flink timestamp. That is harmless
  while only the `ais.enriched` sink follows the tagging operator, and it would matter
  if an event-time operator were ever placed after it.
- `zones_version` on every record ties each tag to a package in `refdata_packages`.
