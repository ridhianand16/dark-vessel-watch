"""ClickHouse client from CLICKHOUSE_* settings (environment, then .env)."""

from __future__ import annotations

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from common.env import setting


def client_from_env(connect_timeout: int = 10, send_receive_timeout: int = 300) -> Client:
    return clickhouse_connect.get_client(
        host=setting("CLICKHOUSE_HOST", "localhost"),
        port=int(setting("CLICKHOUSE_PORT", "8123")),
        username=setting("CLICKHOUSE_USER", "default"),
        password=setting("CLICKHOUSE_PASSWORD", ""),
        connect_timeout=connect_timeout,
        send_receive_timeout=send_receive_timeout,
    )
