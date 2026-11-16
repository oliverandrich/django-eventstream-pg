"""Server-sent events for Django over PostgreSQL LISTEN/NOTIFY."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from django_pg_sse.publish import broadcast, broadcast_json, close_streams

__all__ = ["broadcast", "broadcast_json", "close_streams"]


def __getattr__(name: str) -> Any:
    # publish imports the model, which Django cannot load while it imports INSTALLED_APPS.
    if name in __all__:
        from django_pg_sse import publish

        return getattr(publish, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
