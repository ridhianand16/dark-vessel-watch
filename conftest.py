"""Shared pytest fixtures."""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import psycopg
import pytest
from psycopg import sql

from common.postgis import conninfo


@pytest.fixture
def scratch_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """An empty database on the compose PostGIS, dropped afterwards. Skips when unreachable.

    POSTGRES_DB points at it for the test, so code that builds its DSN from the
    environment (common.postgis.conninfo) uses it. Yields the database name.
    """
    try:
        admin = psycopg.connect(conninfo(), autocommit=True, connect_timeout=2)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostGIS not reachable: {exc}")
    name = f"dvw_test_{uuid.uuid4().hex[:8]}"
    with admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        monkeypatch.setenv("POSTGRES_DB", name)
        try:
            yield name
        finally:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
