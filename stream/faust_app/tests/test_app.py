"""Guards for the one private Faust API the app relies on (as_current_event), and the
exit on a partition mismatch."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, cast

import pytest
from faust.streams import current_event
from faust.types import EventT

from stream.faust_app import app as app_module
from stream.faust_app.app import as_current_event
from stream.faust_app.tracker import PartitionMismatch


class FakeEvent:
    pass


def test_as_current_event_sets_and_restores() -> None:
    outer, inner = cast(EventT[Any], FakeEvent()), cast(EventT[Any], FakeEvent())
    with as_current_event(outer):
        assert current_event() is outer
        with as_current_event(inner):
            assert current_event() is inner
        assert current_event() is outer
    assert current_event() is None


def test_partition_mismatch_exits_70_without_sending(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """An ais.enriched record in another partition than its ais.raw source stops the
    tracker (exit 70) before anything is sent or committed (ADR 0019 point 3)."""
    value = json.dumps({
        "schema_version": 1, "mmsi": 219026147, "run_id": "run-a", "event_ts": 1_761_609_601_000,
        "lat": 56.0, "lon": 10.0, "src_partition": 3, "src_offset": 10, "station_class": "ship",
        "zones_version": "test-1", "eez_id": None, "mpa_ids": [], "in_port": False,
        "dist_eez_boundary_m": None, "dist_mpa_m": None,
    }).encode()
    event = SimpleNamespace(message=SimpleNamespace(key=b"219026147", value=value, partition=4, offset=5))
    sent: list[object] = []

    async def send(**kwargs: object) -> None:
        sent.append(kwargs)

    class Exited(Exception):
        pass

    def fake_exit(code: int) -> None:
        raise Exited(code)

    monkeypatch.setattr(app_module, "cfg", replace(app_module.cfg, input_topic="ais.enriched"))
    monkeypatch.setattr(app_module.tracks_topic, "send", send)
    monkeypatch.setattr(app_module.alerts_topic, "send", send)
    monkeypatch.setattr(os, "_exit", fake_exit)  # app.py calls os._exit at the time of the crash
    with pytest.raises(Exited) as exc:
        asyncio.run(app_module.handle_or_exit([cast(EventT[bytes], event)]))
    assert exc.value.args == (app_module.EXIT_CRASH,) == (70,)
    assert sent == []
    # Exit 70 for this reason, not some other failure in the same path.
    crashes = [r.exc_info[0] for r in caplog.records if r.exc_info is not None]
    assert crashes == [PartitionMismatch]
