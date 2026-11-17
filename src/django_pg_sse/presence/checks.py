"""System checks for the presence settings."""

from collections.abc import Sequence
from typing import Any

from django.apps import AppConfig
from django.core.checks import CheckMessage, Error
from django.utils.module_loading import import_string

from django_pg_sse import conf


def check_presence_topics(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    """PG_SSE["PRESENCE_TOPICS"] must be a list or tuple of prefixes."""
    value = conf.get("PRESENCE_TOPICS")
    # A plain string would match its characters one by one.
    if isinstance(value, list | tuple):
        prefixes: list[Any] | tuple[Any, ...] = value  # pyright: ignore[reportUnknownVariableType]
        if all(isinstance(prefix, str) for prefix in prefixes):
            return []
    return [Error(f'PG_SSE["PRESENCE_TOPICS"] must be a list of topic prefixes, not {value!r}.', id="pg_sse.E006")]


def check_presence_identity(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    """PG_SSE["PRESENCE_IDENTITY"] must name an importable function."""
    value = conf.get("PRESENCE_IDENTITY")
    try:
        if isinstance(value, str) and callable(import_string(value)):
            return []
    except ImportError:
        pass
    return [Error(f'PG_SSE["PRESENCE_IDENTITY"] must name a function, not {value!r}.', id="pg_sse.E007")]
