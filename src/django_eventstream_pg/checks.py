from collections.abc import Sequence
from typing import Any

from django.apps import AppConfig
from django.core.checks import CheckMessage, Error
from django.db import connections


# Django passes both arguments by keyword, so they keep their names although the check ignores them.
def check_postgresql(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    # LISTEN/NOTIFY and advisory locks exist only in PostgreSQL.
    vendor = connections["default"].vendor
    if vendor == "postgresql":
        return []
    return [
        Error(
            f"django-eventstream-pg needs PostgreSQL, but the default database is {vendor}.",
            id="eventstream_pg.E001",
        )
    ]
