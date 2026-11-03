"""A Faust consumer transport that reads with isolation.level=read_committed.

The Flink enrichment job writes ais.enriched in Kafka transactions that commit with
each checkpoint (ADR 0014). A read_uncommitted reader would see records of a
transaction that is later aborted, after a Flink failure, and then their replacements
too. Faust only reads committed when its own processing_guarantee is exactly_once,
which also makes the tracker's producer transactional; the tracker stays at-least-once
(ADR 0004), so only the consumer is changed here. Neither Faust nor its aiokafka
driver has a public setting for the consumer alone (faust-streaming 0.15).

app.py points broker_consumer at the `kafka-rc://` scheme registered below; the
producer keeps the stock `kafka://` transport.

The level is set through a private aiokafka attribute, so it is also checked where it
takes effect: once the consumer has started, its fetcher must be reading committed.
If the attribute is missing or the fetcher reads anything else, the tracker exits
(EXIT_NOT_COMMITTED) before it reads a record. Tests: test_read_committed.py builds the
consumer through Faust, checks the check, and against Redpanda reads a topic with an
aborted and a committed transaction and gets only the committed record.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import faust.transport.drivers
from aiokafka.consumer.fetcher import READ_COMMITTED as FETCHER_READ_COMMITTED  # type: ignore[import-untyped]
from faust.transport.drivers import aiokafka as faust_aiokafka

SCHEME = "kafka-rc"
ISOLATION_LEVEL = "read_committed"
EXIT_NOT_COMMITTED = 78  # EX_CONFIG

log = logging.getLogger("tracker")


class ReadCommittedError(RuntimeError):
    """The consumer would not read committed; aiokafka's internals have changed."""


def set_isolation_level(consumer: Any) -> None:
    """Set read_committed on an aiokafka consumer that has not started yet."""
    if not hasattr(consumer, "_isolation_level"):
        raise ReadCommittedError(
            f"{type(consumer).__name__} has no _isolation_level attribute; cannot read ais.enriched "
            "with read_committed (aiokafka changed; see stream/faust_app/read_committed.py)"
        )
    consumer._isolation_level = ISOLATION_LEVEL


def check_isolation_level(consumer: Any) -> None:
    """Raise unless a started aiokafka consumer's fetcher reads committed."""
    fetcher = getattr(consumer, "_fetcher", None)
    level = getattr(fetcher, "_isolation_level", None)
    if level != FETCHER_READ_COMMITTED:
        raise ReadCommittedError(
            f"consumer fetcher isolation level is {level!r}, expected {FETCHER_READ_COMMITTED} "
            "(read_committed); the tracker would see aborted Flink transactions "
            "(aiokafka changed; see stream/faust_app/read_committed.py)"
        )


class ConsumerThread(faust_aiokafka.AIOKafkaConsumerThread):
    def _create_worker_consumer(self, transport: Any) -> Any:  # aiokafka.AIOKafkaConsumer (untyped)
        consumer = super()._create_worker_consumer(transport)
        # aiokafka hands the isolation level to its fetcher in start(), which Faust calls
        # after this returns; on_start() checks that it arrived.
        set_isolation_level(consumer)
        return consumer

    async def on_start(self) -> None:
        try:
            await super().on_start()  # creates the consumer (above) and starts it
            check_isolation_level(self._consumer)
            log.info("consumer reads with isolation.level=%s", ISOLATION_LEVEL)
        except ReadCommittedError as exc:
            # os._exit: Faust would log an exception raised here and carry on without
            # a consumer, or with one that reads aborted records.
            log.critical("%s; exiting", exc)
            logging.shutdown()
            os._exit(EXIT_NOT_COMMITTED)


class Consumer(faust_aiokafka.Consumer):
    def _new_consumer_thread(self) -> faust_aiokafka.AIOKafkaConsumerThread:
        return ConsumerThread(self, loop=self.loop, beacon=self.beacon)


class Transport(faust_aiokafka.Transport):
    Consumer = Consumer


faust.transport.drivers.TRANSPORTS.aliases[SCHEME] = f"{__name__}:Transport"
