"""Kafka topic -> ClickHouse table sinks, one consumer group per topic (specs.py).

raw:    ais.raw -> dvw.ais_raw, docs/adr/0003-raw-sink-python-consumer.md
alerts: ais.alerts -> dvw.alerts, docs/adr/0006-alerts-sink-config-driven.md
tracks: ais.tracks -> dvw.tracks (and the rollups), docs/adr/0020-rollups-from-tracks-and-ttl.md

Several sinks can share a process: python -m sinks.clickhouse alerts tracks.
"""
