"""Sending events: broadcast(), broadcast_json(), close_streams() and their async variants."""

import contextlib
import functools
import json
from typing import Any

from asgiref.sync import sync_to_async
from django.contrib.auth.base_user import AbstractBaseUser
from django.contrib.sessions.backends.base import SessionBase
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, transaction
from django.http import HttpRequest
from django.utils.asyncio import async_unsafe

from django_pg_sse.messages import CHANNEL, LINE_BREAK, RESERVED_EVENTS, RESERVED_PREFIX, Owner
from django_pg_sse.models import EVENT_MAX_LENGTH, LATEST_MAX_LENGTH, TOPIC_MAX_LENGTH, StreamEvent

# Serialises event writers, so ids become visible in the order they are assigned.
# Replay after Last-Event-ID would skip an event whose smaller id committed late.
PUBLISH_LOCK_KEY = int.from_bytes(b"sse-emit", signed=True)


# async_unsafe: from a running event loop, the robust on_commit callback could only log the
# failure, and the event would be lost; the a* variants below serve async code.
@async_unsafe
def broadcast(topic: str, data: str, event: str = "", latest: str = "") -> None:
    """Send `data` to every stream subscribed to `topic`, once the current transaction commits.

    A rollback sends nothing. htmx swaps `data` as HTML. With a name in `event`, htmx dispatches
    a DOM event of that name instead, and `data` may be any text.

    `latest` marks a snapshot, such as an online list that replaces the previous one.
    A replay, including the hub's catch-up, skips a snapshot when a newer one has the same topic and key.

    Raises ValueError for a topic starting with `_`, a topic over 200 characters, an event name or
    latest key over 100 characters, either with a line break, and the event names `closed` and
    `hx:release`. Raises SynchronousOnlyOperation in a running event loop; use abroadcast() there.
    """
    if topic.startswith(RESERVED_PREFIX):
        raise ValueError(f"Topics starting with {RESERVED_PREFIX!r} are reserved.")
    # The database would refuse a longer value after the commit, where it is only logged.
    _check_length("An SSE topic", topic, TOPIC_MAX_LENGTH)
    _check_name("An SSE event name", event, EVENT_MAX_LENGTH)
    _check_name("A latest key", latest, LATEST_MAX_LENGTH)
    if event in RESERVED_EVENTS:
        raise ValueError(f"The event name {event!r} controls the stream.")
    _broadcast(topic, data, event, latest)


@async_unsafe
def broadcast_json(topic: str, value: Any, *, event: str, latest: str = "") -> None:
    """Send `value` as JSON in an event named `event`, like broadcast().

    DjangoJSONEncoder encodes it, so dates, decimals and UUIDs work.

    Raises ValueError without an event name and for NaN or Infinity, which JSON.parse() rejects,
    besides the cases of broadcast(). Raises SynchronousOnlyOperation in a running event loop.
    """
    # htmx would swap a message without an event name into the page as HTML.
    if not event:
        raise ValueError("broadcast_json() needs an event name.")
    # allow_nan=False: JSON.parse() in the browser rejects NaN and Infinity.
    data = json.dumps(value, cls=DjangoJSONEncoder, separators=(",", ":"), allow_nan=False)
    broadcast(topic, data, event, latest)


def _check_length(label: str, value: str, limit: int) -> None:
    if len(value) > limit:
        raise ValueError(f"{label} has at most {limit} characters, not {len(value)}.")


def _check_name(label: str, value: str, limit: int) -> None:
    _check_length(label, value, limit)
    # A line break would end the SSE field early.
    if LINE_BREAK.search(value):
        raise ValueError(f"{label} cannot contain a line break.")


def _broadcast(topic: str, data: str, event: str = "", latest: str = "") -> None:
    # A function, not functools.partial: Django before 6.1 logs a robust callback's failure
    # through its __qualname__, which partial objects lack.
    def publish() -> None:
        _publish(topic, data, event, latest)

    # robust: the caller's transaction has committed, so a failure here is logged, not raised.
    transaction.on_commit(publish, robust=True)


def _publish(topic: str, data: str, event: str, latest: str) -> None:
    # on_commit runs this in autocommit. Inside an outer transaction, a savepoint keeps a
    # failed statement from aborting that transaction.
    savepoint = transaction.atomic() if connection.in_atomic_block else contextlib.nullcontext()
    with savepoint, connection.cursor() as cursor:
        cursor.execute(_publish_sql(), [PUBLISH_LOCK_KEY, topic, event, data, latest, CHANNEL])


@functools.cache
def _publish_sql() -> str:
    """One statement: lock, store and notify.

    The INSERT reads from `locked`, so the event takes its id under the lock.
    The statement's transaction holds the lock until it commits.
    clock_timestamp() runs under the lock, unlike now(), which is read when the transaction starts.
    The payload matches notify_payload().
    """
    table = connection.ops.quote_name(StreamEvent._meta.db_table)
    return f"""
        WITH locked AS (SELECT pg_advisory_xact_lock(%s)),
        stored AS (
            INSERT INTO {table} (topic, event, data, latest, created)
            SELECT %s, %s, %s, %s, clock_timestamp() FROM locked
            RETURNING id, topic
        )
        SELECT pg_notify(%s, id || ':' || topic) FROM stored
    """


@async_unsafe
def close_streams(*, user: AbstractBaseUser | None = None, session: SessionBase | None = None) -> None:
    """End the open streams of `user` or of one session, once the current transaction commits.

    Each stream receives an event named "closed" and ends. htmx reconnects, and the subscription
    check runs again, so call this after taking away a user's access to a topic.

    Raises ValueError without a user and a session, and for an anonymous user.
    Raises SynchronousOnlyOperation in a running event loop; use aclose_streams() there.
    """
    if user is None and session is None:
        raise ValueError("close_streams() needs a user or a session.")
    # An anonymous user has no pk, so its close topic would name no stream.
    if user is not None and user.pk is None:
        raise ValueError("close_streams() needs a saved user, not an anonymous one.")
    # A session without a key was never saved, so no stream was opened with it.
    owner = Owner.of(
        user=str(user.pk) if user is not None else None,
        session=session.session_key if session is not None else None,
    )
    for topic in owner.close_topics:
        _broadcast(topic, "")


# The sync functions use the database connection, so they run in Django's sync thread.
async def abroadcast(topic: str, data: str, event: str = "", latest: str = "") -> None:
    """broadcast() for async code. It runs in Django's sync thread and raises what broadcast() raises."""
    await sync_to_async(broadcast)(topic, data, event, latest)


async def abroadcast_json(topic: str, value: Any, *, event: str, latest: str = "") -> None:
    """broadcast_json() for async code. It raises what broadcast_json() raises."""
    await sync_to_async(broadcast_json)(topic, value, event=event, latest=latest)


async def aclose_streams(*, user: AbstractBaseUser | None = None, session: SessionBase | None = None) -> None:
    """close_streams() for async code. It raises what close_streams() raises."""
    await sync_to_async(close_streams)(user=user, session=session)


# Signals pass sender by keyword, so it keeps its name although the receiver ignores it.
def close_session_streams(sender: Any, request: HttpRequest | None, **kwargs: Any) -> None:  # noqa: ARG001
    """A user_logged_out receiver: other tabs of the session lose the user's topics."""
    if request is not None:
        close_streams(session=request.session)
