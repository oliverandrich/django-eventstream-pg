"""Who has a topic open: optional, with "django_pg_sse.presence" in INSTALLED_APPS."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from django.dispatch import Signal
from django.http import HttpRequest

if TYPE_CHECKING:
    from django_pg_sse.presence.tracker import apresent, present

__all__ = ["Presence", "apresent", "presence_changed", "present", "user_identity"]


@dataclass(frozen=True)
class Presence:
    """One reader of a topic, however many tabs it has open."""

    #: The first value PRESENCE_IDENTITY returned, such as "user:7".
    identity: str
    #: The second value PRESENCE_IDENTITY returned, such as {"name": "ada"}.
    meta: dict[str, Any] = field(default_factory=dict)


#: Sent with sender=Presence, topic, joined and left, in the worker that saw the change.
#: joined and left are tuples of Presence; one of them is empty.
presence_changed = Signal()


def user_identity(request: HttpRequest) -> tuple[str, dict[str, Any]] | None:
    """The default PRESENCE_IDENTITY: a logged-in user by primary key and username, else None."""
    user = request.user
    if not user.is_authenticated:
        return None
    return f"user:{user.pk}", {"name": user.get_username()}


def __getattr__(name: str) -> Any:
    # tracker imports the model, which Django cannot load while it imports INSTALLED_APPS.
    if name in {"present", "apresent"}:
        from django_pg_sse.presence import tracker

        return getattr(tracker, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
