"""The tracker's consumer reads ais.enriched with read_committed (read_committed.py)."""

from __future__ import annotations

import asyncio
import importlib
import inspect

import faust

from stream.faust_app import read_committed


def test_worker_consumer_reads_committed() -> None:
    async def build() -> object:
        app = faust.App(
            "dvw-test-rc",
            broker="kafka://localhost:1",
            broker_consumer=f"{read_committed.SCHEME}://localhost:1",
            web_enabled=False,
        )
        assert isinstance(app.transport, read_committed.Transport)
        consumer = app.consumer
        assert isinstance(consumer, read_committed.Consumer)
        thread = consumer._new_consumer_thread()
        return thread._create_worker_consumer(app.transport)

    kafka_consumer = asyncio.run(build())
    # The value aiokafka hands to its fetcher in start().
    assert getattr(kafka_consumer, "_isolation_level") == "read_committed"


def test_aiokafka_passes_the_attribute_to_its_fetcher() -> None:
    """read_committed.py sets a private attribute; this fails if aiokafka stops using it."""
    consumer_cls = importlib.import_module("aiokafka").AIOKafkaConsumer
    assert "isolation_level=self._isolation_level" in inspect.getsource(consumer_cls.start)
