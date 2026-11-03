"""Guards for the one private Faust API the app relies on (as_current_event)."""

from __future__ import annotations

from typing import Any, cast

from faust.streams import current_event
from faust.types import EventT

from stream.faust_app.app import as_current_event


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
