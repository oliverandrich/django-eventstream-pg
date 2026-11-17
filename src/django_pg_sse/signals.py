"""Signals for the lifecycle of a stream, such as for presence or metrics."""

from dataclasses import dataclass, field

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
