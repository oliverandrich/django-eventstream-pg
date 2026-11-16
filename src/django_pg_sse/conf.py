"""The PG_SSE setting, a dict whose keys override these defaults."""

from collections.abc import Callable
from typing import Any

from django.conf import settings
from django.http import HttpRequest
from django.utils.module_loading import import_string

DEFAULTS: dict[str, Any] = {
    # Dotted path to a function (request, topic) -> bool that decides who may read a topic.
    "CAN_SUBSCRIBE": "django_pg_sse.permissions.can_subscribe",
    # How long prune_stream_events keeps events, which is how far back Last-Event-ID reaches.
    "RETENTION_HOURS": 24,
    # Connections per ASGI worker for the replay and revocation queries of opening streams.
    "STREAM_CONNECTIONS": 4,
    # Seconds after which a query of the hub gives up; a half-open connection would wait minutes.
    "QUERY_TIMEOUT": 30,
}


def get(name: str) -> Any:
    """The value of `name` in PG_SSE, or its default."""
    return {**DEFAULTS, **getattr(settings, "PG_SSE", {})}[name]


def can_subscribe() -> Callable[[HttpRequest, str], bool]:
    """The policy function that PG_SSE["CAN_SUBSCRIBE"] names."""
    return import_string(get("CAN_SUBSCRIBE"))
