"""Kafka topic -> ClickHouse table sinks, one process per topic (specs.py).

raw:    ais.raw -> dvw.ais_raw, docs/adr/0003-raw-sink-python-consumer.md
alerts: ais.alerts -> dvw.alerts, docs/adr/0006-alerts-sink-config-driven.md
"""
