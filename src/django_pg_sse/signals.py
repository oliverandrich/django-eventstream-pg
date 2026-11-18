"""Signals for the lifecycle of a stream, such as for presence or metrics."""

import asyncio
import contextvars
from dataclasses import dataclass, field
from typing import Any

from asgiref.sync import SyncToAsync, sync_to_async
from django.db import DEFAULT_DB_ALIAS, connections
from django.dispatch import Signal
from django.http import HttpRequest

from django_pg_sse import conf


@dataclass(frozen=True)
class StreamInfo:
    """One open stream, the `stream` argument of both signals."""

    #: A new random id for each connection; a reconnect of the same tab gets another.
    id: str
    #: The user's primary key as a string, or None for an anonymous stream.
    user_pk: str | None
    #: The SHA-256 digest of the session key, never the key; None for an anonymous stream.
    session: str | None
    #: The topics of the stream, in the order of the query string.
    topics: tuple[str, ...]
    #: The request that opened the stream. Its session and user load lazily, so read them in sync code.
    request: HttpRequest = field(compare=False, repr=False)


#: Sent with sender=StreamInfo once a stream's subscription is in place, before its first message.
stream_opened = Signal()
#: Sent with sender=StreamInfo when the stream ends in a running process. A killed process sends nothing.
stream_closed = Signal()


async def asend(signal: Signal, sender: Any, **named: Any) -> None:
    """Send `signal` from async code to its receivers outside the request's sync thread.

    The request's thread would run the receivers' sync code, and that of every task they start.
    For a stream, that thread keeps its connection until the client leaves.
    Under an ASGI server they use asgiref's shared sync thread instead, then close_connection() runs there.
    The rest of the context stays, such as the active language and the script prefix.

    asend_robust() logs a failing receiver to django.dispatch, so the caller goes on.
    """
    if not signal.has_listeners(sender):
        return
    # Cancelling the caller cancels the awaited task as well.
    await asyncio.get_running_loop().create_task(_send(signal, sender, named), context=outside_request_thread())


async def _send(signal: Signal, sender: Any, named: dict[str, Any]) -> None:
    await signal.asend_robust(sender=sender, **named)
    # A sync receiver may have opened the shared thread's connections.
    await sync_to_async(close_connection)()


def outside_request_thread() -> contextvars.Context:
    """A copy of the current context in which sync_to_async() leaves the request's thread."""
    context = contextvars.copy_context()
    # The ASGI handler's ThreadSensitiveContext binds sync code to the request's thread.
    context.run(SyncToAsync.thread_sensitive_context.set, None)  # pyright: ignore[reportArgumentType]
    return context


def close_connection() -> None:
    """Close the calling thread's connections to default and PG_SSE["DATABASE"].

    Run it in that thread through sync_to_async. With Django's pool, closing returns a connection
    to the pool; without one, it disconnects. A connection in an atomic block stays open.
    """
    for alias in {DEFAULT_DB_ALIAS, conf.get("DATABASE")}:
        # connections[alias] is the calling thread's connection, not the event loop's.
        database = connections[alias]
        # close() would roll the block back and drop its on_commit callbacks, such as a broadcast.
        if not database.in_atomic_block:
            database.close()
