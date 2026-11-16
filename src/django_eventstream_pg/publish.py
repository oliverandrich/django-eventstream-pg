"""Sending events: broadcast() and close_streams()."""

from typing import Any

from django.contrib.auth.base_user import AbstractBaseUser
from django.contrib.sessions.backends.base import SessionBase
from django.db import connection, transaction
from django.db.backends.utils import CursorWrapper
from django.http import HttpRequest

from django_eventstream_pg.messages import CHANNEL, LINE_BREAK, RESERVED_PREFIX, Owner, notify_payload
from django_eventstream_pg.models import StreamEvent

# Serialises event writers, so ids become visible in the order they are assigned.
# Replay after Last-Event-ID would skip an event whose smaller id committed late.
PUBLISH_LOCK_KEY = int.from_bytes(b"sse-emit", signed=True)


def broadcast(topic: str, html: str, event: str = "") -> None:
    """Send `html` to every stream subscribed to `topic`, once the current transaction commits.

    A rollback sends nothing. With a name in `event`, htmx dispatches a DOM event instead of
    swapping the html.
    """
    if topic.startswith(RESERVED_PREFIX):
        raise ValueError(f"Topics starting with {RESERVED_PREFIX!r} are reserved.")
    if LINE_BREAK.search(event):
        raise ValueError("An SSE event name cannot contain a line break.")
    _broadcast(topic, html, event)


def _broadcast(topic: str, html: str, event: str = "") -> None:
    # A function, not functools.partial: Django before 6.1 logs a robust callback's failure
    # through its __qualname__, which partial objects lack.
    def publish() -> None:
        _publish(topic, html, event)

    # robust: the caller's transaction has committed, so a failure here is logged, not raised.
    transaction.on_commit(publish, robust=True)


def _publish(topic: str, html: str, event: str) -> None:
    with transaction.atomic(), connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [PUBLISH_LOCK_KEY])
        stored = StreamEvent.objects.create(topic=topic, event=event, html=html)
        _notify(cursor, stored.id, topic)


def _notify(cursor: CursorWrapper, event_id: int, topic: str) -> None:
    cursor.execute("SELECT pg_notify(%s, %s)", [CHANNEL, notify_payload(event_id, topic)])


def close_streams(*, user: AbstractBaseUser | None = None, session: SessionBase | None = None) -> None:
    """End the open streams of `user` or of one session, once the current transaction commits.

    Each stream receives an event named "closed" and ends. htmx reconnects, and the subscription
    check runs again, so call this after taking away a user's access to a topic.
    """
    if user is None and session is None:
        raise ValueError("close_streams() needs a user or a session.")
    # A session without a key was never saved, so no stream was opened with it.
    owner = Owner.of(
        user=str(user.pk) if user is not None else None,
        session=session.session_key if session is not None else None,
    )
    for topic in owner.close_topics:
        _broadcast(topic, "")


# Signals pass sender by keyword, so it keeps its name although the receiver ignores it.
def close_session_streams(sender: Any, request: HttpRequest | None, **kwargs: Any) -> None:  # noqa: ARG001
    """A user_logged_out receiver: other tabs of the session lose the user's topics."""
    if request is not None:
        close_streams(session=request.session)
