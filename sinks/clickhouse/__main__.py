"""python -m sinks.clickhouse <name>: run one topic -> table sink (see sink.py)."""

import argparse
import logging
import signal
import sys

from confluent_kafka import Consumer

from common.clickhouse import client_from_env
from common.env import setting
from common.kafka_batch import consumer_config
from sinks.clickhouse.sink import ClickHouseSink
from sinks.clickhouse.specs import SPECS


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="sinks.clickhouse", description="Run one topic -> ClickHouse table sink.")
    p.add_argument("name", choices=sorted(SPECS), help="which sink to run")
    spec = SPECS[p.parse_args(argv).name]

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log = logging.getLogger(f"{spec.name}-sink")
    brokers = setting("REDPANDA_BROKERS", "localhost:19092")
    # Short timeouts keep a full retry cycle (6 attempts) well under max.poll.interval.ms.
    client = client_from_env(connect_timeout=5, send_receive_timeout=30)
    if not client.command(f"EXISTS TABLE {spec.table}"):
        log.error("%s does not exist. Run: make migrate", spec.table)
        return 1
    sink = ClickHouseSink(spec, Consumer(consumer_config(spec.group_id, brokers)), client)
    signal.signal(signal.SIGTERM, sink.request_stop)
    signal.signal(signal.SIGINT, sink.request_stop)
    log.info("consuming %s as group %s from %s into %s", spec.topic, spec.group_id, brokers, spec.table)
    return sink.run()


if __name__ == "__main__":
    sys.exit(main())
