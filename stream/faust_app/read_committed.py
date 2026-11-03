"""A Faust consumer transport that reads with isolation.level=read_committed.

The Flink enrichment job writes ais.enriched in Kafka transactions that commit with
each checkpoint (ADR 0014). A read_uncommitted reader would see records of a
transaction that is later aborted, after a Flink failure, and then their replacements
too. Faust only reads committed when its own processing_guarantee is exactly_once,
which also makes the tracker's producer transactional; the tracker stays at-least-once
(ADR 0004), so only the consumer is changed here.

app.py points broker_consumer at the `kafka-rc://` scheme registered below; the
producer keeps the stock `kafka://` transport.
"""

from __future__ import annotations

from typing import Any

import faust.transport.drivers
from faust.transport.drivers import aiokafka as faust_aiokafka

SCHEME = "kafka-rc"
ISOLATION_LEVEL = "read_committed"


class ConsumerThread(faust_aiokafka.AIOKafkaConsumerThread):
    def _create_worker_consumer(self, transport: Any) -> Any:  # aiokafka.AIOKafkaConsumer (untyped)
        consumer = super()._create_worker_consumer(transport)
        # aiokafka hands the isolation level to its fetcher in start(), which Faust calls
        # after this returns. Private attribute: test_read_committed.py builds a consumer
        # through Faust and checks that start() still passes it on, so an upgrade fails a test.
        consumer._isolation_level = ISOLATION_LEVEL
        return consumer


class Consumer(faust_aiokafka.Consumer):
    def _new_consumer_thread(self) -> faust_aiokafka.AIOKafkaConsumerThread:
        return ConsumerThread(self, loop=self.loop, beacon=self.beacon)


class Transport(faust_aiokafka.Transport):
    Consumer = Consumer


faust.transport.drivers.TRANSPORTS.aliases[SCHEME] = f"{__name__}:Transport"
