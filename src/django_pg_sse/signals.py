"""Signals for the lifecycle of a stream, such as for presence or metrics."""

from dataclasses import dataclass, field
from typing import Any

from asgiref.sync import sync_to_async
from django.db import connection
from django.dispatch import Signal
from django.http import HttpRequest


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
    """Send `signal` from async code to its receivers, then close the request thread's connection.

    asend_robust() logs a failing receiver to django.dispatch, so the caller goes on.
    """
    if not signal.has_listeners(sender):
        return
    await signal.asend_robust(sender=sender, **named)
    # A sync receiver ran in the request's thread and may have opened its connection again.
    await sync_to_async(close_connection)()


def close_connection() -> None:
    """Close the calling thread's database connection; run it in that thread through sync_to_async.

    Under ASGI the request's thread keeps its connection until the client leaves.
    Closing it returns it to the pool.
    """
    # The proxy resolves here, in the calling thread, not in the event loop's.
    connection.close()
