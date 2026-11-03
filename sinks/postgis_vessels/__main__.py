"""python -m sinks.postgis_vessels: run the ais.tracks -> vessels_current sink (see sink.py)."""

import logging
import signal
import sys

from confluent_kafka import Consumer

from common.env import setting
from common.kafka_batch import consumer_config
from sinks.postgis_vessels.sink import GROUP_ID, NAME, TOPIC, VesselsSink, connect


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log = logging.getLogger(f"{NAME}-sink")
    brokers = setting("REDPANDA_BROKERS", "localhost:19092")
    with connect() as conn:
        if conn.execute("SELECT to_regclass('public.vessels_current')").fetchone() == (None,):
            log.error("vessels_current does not exist. Run: make migrate-postgis")
            return 1
    sink = VesselsSink(Consumer(consumer_config(GROUP_ID, brokers)))
    signal.signal(signal.SIGTERM, sink.request_stop)
    signal.signal(signal.SIGINT, sink.request_stop)
    log.info("consuming %s as group %s from %s into vessels_current", TOPIC, GROUP_ID, brokers)
    try:
        return sink.run()
    finally:
        sink.close()


if __name__ == "__main__":
    sys.exit(main())
