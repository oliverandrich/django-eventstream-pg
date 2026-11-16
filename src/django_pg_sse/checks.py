from collections.abc import Sequence
from typing import Any

from django.apps import AppConfig
from django.core.checks import CheckMessage, Error
from django.db import connections

from django_pg_sse import conf


# Django passes both arguments by keyword, so they keep their names although the check ignores them.
def check_postgresql(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    # LISTEN/NOTIFY and advisory locks exist only in PostgreSQL.
    vendor = connections["default"].vendor
    if vendor == "postgresql":
        return []
    return [
        Error(
            f"django-pg-sse needs PostgreSQL, but the default database is {vendor}.",
            id="pg_sse.E001",
        )
    ]


def check_stream_connections(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    value = conf.get("STREAM_CONNECTIONS")
    # bool is an int subclass, and True would mean one connection by accident.
    if isinstance(value, int) and not isinstance(value, bool) and value >= 1:
        return []
    return [
        Error(
            f'PG_SSE["STREAM_CONNECTIONS"] must be a positive integer, not {value!r}.',
            id="pg_sse.E002",
        )
    ]
