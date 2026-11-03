"""python -m sinks.clickhouse <name> [<name> ...]: run topic -> table sinks (see sink.py).

One name runs that sink in the main thread. Several run in one process, one thread
and one consumer group each, and stop together if any fails (common/sink_group.py).
"""

import argparse
import logging
import signal
import sys

from confluent_kafka import Consumer

from common.clickhouse import client_from_env
from common.env import setting
from common.kafka_batch import consumer_config
from common.sink_group import run_together
from sinks.clickhouse.sink import ClickHouseSink
from sinks.clickhouse.specs import SPECS


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="sinks.clickhouse", description="Run topic -> ClickHouse table sinks.")
    p.add_argument("names", nargs="+", choices=sorted(SPECS), metavar="name", help=f"one or more of {sorted(SPECS)}")
    names: list[str] = list(dict.fromkeys(p.parse_args(argv).names))

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log = logging.getLogger("sinks.clickhouse")
    brokers = setting("REDPANDA_BROKERS", "localhost:19092")
    sinks: list[ClickHouseSink] = []
    for name in names:
        spec = SPECS[name]
        # One client per sink: a clickhouse-connect client is not safe to share across
        # threads. Short timeouts keep a full retry cycle (6 attempts) well under
        # max.poll.interval.ms.
        client = client_from_env(connect_timeout=5, send_receive_timeout=30)
        if not client.command(f"EXISTS TABLE {spec.table}"):
            log.error("%s does not exist. Run: make migrate", spec.table)
            return 1
        sinks.append(ClickHouseSink(spec, Consumer(consumer_config(spec.group_id, brokers)), client))
        log.info("%s: consuming %s as group %s from %s into %s", name, spec.topic, spec.group_id, brokers, spec.table)

    if len(sinks) == 1:
        sink = sinks[0]
        signal.signal(signal.SIGTERM, sink.request_stop)
        signal.signal(signal.SIGINT, sink.request_stop)
        return sink.run()
    return run_together(sinks)


if __name__ == "__main__":
    sys.exit(main())
