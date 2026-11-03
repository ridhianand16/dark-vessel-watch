"""PostGIS connection settings, built from the same .env keys the container reads."""

from __future__ import annotations

from psycopg.conninfo import make_conninfo

from common.env import setting


def conninfo() -> str:
    return make_conninfo(
        host=setting("POSTGRES_HOST", "localhost"),
        port=setting("POSTGRES_PORT", "5432"),
        user=setting("POSTGRES_USER", "dvw"),
        password=setting("POSTGRES_PASSWORD", ""),
        dbname=setting("POSTGRES_DB", "dvw"),
    )
