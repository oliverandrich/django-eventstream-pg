"""System checks for the database, the PG_SSE setting and the middleware."""

from collections.abc import Sequence
from typing import Any

from django.apps import AppConfig, apps
from django.conf import settings
from django.core.checks import CheckMessage, Error, Warning  # noqa: A004
from django.db import DEFAULT_DB_ALIAS, connections
from django.utils.module_loading import import_string

from django_pg_sse import conf


def is_positive_int(value: Any) -> bool:
    """Whether `value` is an int of at least 1, excluding bool."""
    # bool is an int subclass, and True would mean one by accident.
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def is_positive_number(value: Any) -> bool:
    """Whether `value` is an int or float above 0, excluding bool."""
    return isinstance(value, int | float) and not isinstance(value, bool) and value > 0


# Django passes both arguments by keyword, so they keep their names although the checks ignore them.
def check_postgresql(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    """pg_sse.E001: the default database and the hub's database must be PostgreSQL."""
    # LISTEN/NOTIFY and advisory locks exist only in PostgreSQL.
    # check_hub_database() reports a hub alias that DATABASES lacks.
    aliases: set[str] = {DEFAULT_DB_ALIAS}
    if _defined(hub := conf.get("DATABASE")):
        aliases.add(hub)
    return [
        Error(
            f"django-pg-sse needs PostgreSQL, but the database {alias!r} is {connections[alias].vendor}.",
            id="pg_sse.E001",
        )
        for alias in sorted(aliases)
        if connections[alias].vendor != "postgresql"
    ]


def check_hub_database(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    """pg_sse.E005: PG_SSE["DATABASE"] must name an alias in DATABASES."""
    alias = conf.get("DATABASE")
    if _defined(alias):
        return []
    return [Error(f'PG_SSE["DATABASE"] names {alias!r}, which DATABASES does not define.', id="pg_sse.E005")]


def check_stream_connections(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    """pg_sse.E002: PG_SSE["STREAM_CONNECTIONS"] must be a positive integer."""
    return _positive_int_setting("STREAM_CONNECTIONS", "pg_sse.E002")


def check_retention_hours(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    """pg_sse.E003: PG_SSE["RETENTION_HOURS"] must be a positive integer."""
    # Zero or a negative value puts the cutoff at or after now, and pruning deletes current events.
    return _positive_int_setting("RETENTION_HOURS", "pg_sse.E003")


def check_query_timeout(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    """pg_sse.E004: PG_SSE["QUERY_TIMEOUT"] must be a positive number of seconds."""
    value = conf.get("QUERY_TIMEOUT")
    if is_positive_number(value):
        return []
    return [Error(f'PG_SSE["QUERY_TIMEOUT"] must be a positive number of seconds, not {value!r}.', id="pg_sse.E004")]


def check_middleware(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    """pg_sse.W001: MIDDLEWARE should contain AuthenticationMiddleware or a subclass of it."""
    # The stream view and the default CAN_SUBSCRIBE read request.user.
    if apps.is_installed("django.contrib.auth"):
        # Imported here: the middleware imports auth's models, which need the auth app.
        from django.contrib.auth.middleware import AuthenticationMiddleware

        for path in settings.MIDDLEWARE:
            try:
                middleware = import_string(path)
            except ImportError:
                # Django's own checks report a middleware that cannot be imported.
                continue
            if isinstance(middleware, type) and issubclass(middleware, AuthenticationMiddleware):
                return []
    return [
        Warning(
            "django-pg-sse reads request.user, but MIDDLEWARE has no AuthenticationMiddleware.",
            hint="Add django.contrib.auth.middleware.AuthenticationMiddleware, or silence this check "
            "if another middleware sets request.user.",
            id="pg_sse.W001",
        )
    ]


def _defined(alias: Any) -> bool:
    # A dict in place of the alias name would make `in connections` raise TypeError.
    return isinstance(alias, str) and alias in connections


def _positive_int_setting(name: str, check_id: str) -> list[CheckMessage]:
    value = conf.get(name)
    if is_positive_int(value):
        return []
    return [Error(f"PG_SSE[{name!r}] must be a positive integer, not {value!r}.", id=check_id)]
