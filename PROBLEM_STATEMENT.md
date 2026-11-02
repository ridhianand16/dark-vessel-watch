# Dark Vessel Watch

A self-hosted streaming pipeline that detects suspicious ship behaviour from AIS data, built to run offline with zero message loss.

## Problem

Ships broadcast position, speed and identity over AIS. Vessels doing illegal fishing, sanctions evasion or smuggling hide by switching AIS off ("going dark"), faking positions (spoofing) or sharing an identity (MMSI) with another ship. Research by Global Fishing Watch found intentional switch-offs cluster near EEZ boundaries and protected areas.

Detecting this is a streaming problem: per-vessel state, millions of messages a day, alerts within seconds, and a full history for investigation. In defence and regulated settings it must also run air-gapped, with no cloud services and no data loss.

## Goal

Ingest a day of real AIS traffic, enrich it, track every vessel with a Kalman filter, raise three kinds of alerts in near real time, and store raw plus downsampled history. Prove with a reconciliation job that no message was lost.

## Non-goals

- No frontend beyond a simple map/Grafana view for the demo
- No auth, API gateway or managed cloud services
- No ML model training. Detection is rule-based and stateful.

## Data

| Source | Use |
|---|---|
| Danish Maritime Authority historical AIS (aisdata.ais.dk), daily zipped CSV | Position stream (timestamp, MMSI, lat, lon, SOG, COG, heading) plus static fields (name, ship type, size) |
| Marine Regions EEZ boundaries | Zone tagging, boundary distance |
| Natura 2000 / WDPA marine protected areas | Sensitive zones |
| World Port Index | Port zones (suppress gap alerts in port) |
| Injected synthetic anomalies (labelled) | Guaranteed demo events, precision/recall measurement |

Reference data ships as an offline package: versioned, SHA-256 checksummed, signature-verified before loading into PostGIS.

## Architecture

```
CSV ──► replayer ──► Redpanda: ais.raw
                         │
                         ▼
                  Flink (Java, DataStream)          ──► ais.dlq (invalid records)
                  validate · dedup · zone tagging
                  via broadcast state · exactly-once
                         │
                         ▼
                  Redpanda: ais.enriched
                         │
                         ▼
                  Faust (Python, RocksDB state)
                  per-MMSI Kalman filter · gap detection
                  · teleport · MMSI clash · scoring
                    │                    │
                    ▼                    ▼
              ais.tracks            ais.alerts
                    │                    │
                    ▼                    ▼
   ClickHouse: raw → 1-min → 1-hour (MVs, TTL tiers), alerts
   PostGIS:    zones, vessels_current (upsert), reference package registry
```

Day 1 skips Flink: Faust consumes `ais.raw` directly. Flink is inserted later without changing Faust's contract.

## Replayer

Reads a day's CSV sorted by timestamp, sleeps for the real time gap divided by a speed factor (1x to 100x), and produces to `ais.raw` keyed by MMSI. It models the diode-only sensor feed. It can also mix in labelled synthetic anomalies at known times. Optional `--source live` reads from the AISStream.io WebSocket.

## Detections

| Alert | Rule (initial) |
|---|---|
| DARK_GAP | No report for > 30 min from a vessel last moving > 2 kn, last seen within N nm of an EEZ boundary or protected area, not in port, and not near the receiver coverage edge (to separate coverage gaps from switch-offs). Kalman-predicted position attached to the alert. |
| TELEPORT | Implied speed between consecutive reports > 50 kn, or Kalman innovation above a threshold |
| MMSI_CLASH | Same MMSI reporting two tracks that cannot be one vessel (repeated impossible jumps that alternate between positions) |

Invalid records (bad MMSI, lat/lon out of range, unparseable) go to `ais.dlq` with a reason code.

## Success metrics

- **Zero loss:** reconciliation proves `produced = stored + DLQ` per partition offset range
- **Latency:** p99 end-to-end (replayer send to alert written) under 2 s at 50x replay
- **Throughput:** sustained events/sec on a laptop, recorded in the README
- **Detection:** precision and recall on the injected anomaly set
- **Resilience:** killing a Faust worker or Flink TaskManager mid-run causes no loss or duplicate alerts

## Milestones

| # | Deliverable |
|---|---|
| M0 | Docker Compose up: Redpanda, Console, ClickHouse, PostGIS |
| M1 | Replayer → `ais.raw` → raw table in ClickHouse |
| M2 | Faust: Kalman tracking, DARK_GAP, TELEPORT, alerts topic and table |
| M3 | PostGIS zones from a checksummed offline package, `vessels_current` upsert |
| M4 | Flink job: validation, dedup, zone tagging via broadcast state, exactly-once |
| M5 | ClickHouse MVs + TTL tiers, reconciliation job |
| M6 | Observability: Prometheus, Grafana, consumer lag, latency histograms, trace_id propagation |
| M7 | k3d/kind + Helm deployment |
| M8 | README with numbers, architecture diagram, ADRs, 2-min demo video |

Day 1 target: M0 to M2.

## Demo script (2 minutes)

1. `docker compose up`, replay one day at 50x
2. Redpanda Console: topics filling, consumer lag flat
3. Map: vessels moving, an injected ship goes dark near a protected area, alert fires with predicted position
4. Kill a Faust worker, it recovers, reconciliation query still shows zero loss
5. ClickHouse: same vessel's history from raw, 1-min and 1-hour tables
