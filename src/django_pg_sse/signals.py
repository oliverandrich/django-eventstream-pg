"""Signals for the lifecycle of a stream, such as for presence or metrics."""

from dataclasses import dataclass

from django.dispatch import Signal


@dataclass(frozen=True)
class StreamInfo:
    """One open stream, the `stream` argument of both signals."""

    # A new id for each connection; a reconnect of the same tab gets another.
    id: str
    # None for an anonymous stream.
    user_pk: str | None
    # The SHA-256 digest of the session key, never the key; None for an anonymous stream.
    session: str | None
    topics: tuple[str, ...]


# Both signals pass sender=StreamInfo.
# Sent once the stream's subscription is in place, before its first message.
stream_opened = Signal()
# Sent when the stream ends in a running process. A killed process sends nothing.
stream_closed = Signal()
