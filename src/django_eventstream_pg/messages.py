"""SSE message format, NOTIFY payloads and the values streams exchange with the hub."""

import hashlib
import re
from dataclasses import dataclass
from enum import Enum
from functools import cached_property

# The PostgreSQL channel that carries "<id>:<topic>" for every stored event.
CHANNEL = "eventstream_pg"
# close_streams() stores its targets as events on this topic, so they replay like any event.
# The view refuses it before CAN_SUBSCRIBE runs.
CLOSE_TOPIC = "_close"

# SSE ends a line at CRLF, CR or LF, so each of them must start a new data field.
LINE_BREAK = re.compile(r"\r\n|\r|\n")

CONNECTED = ": connected\n\n"
KEEPALIVE = ": keepalive\n\n"
CLOSED = "event: closed\ndata: \n\n"


@dataclass(frozen=True)
class Message:
    id: int
    topic: str
    event: str
    html: str

    @cached_property
    def sse(self) -> str:
        # Formatted once, however many streams receive the message.
        return format_event(self)


class Signal(Enum):
    # The client fell too far behind. The stream ends, and htmx reconnects with Last-Event-ID,
    # so the replay delivers what the queue dropped.
    OVERFLOW = "overflow"
    # close_streams() named the stream. It sends CLOSED and ends.
    REVOKED = "revoked"


@dataclass(frozen=True)
class Owner:
    """The targets close_streams() matches against a stream."""

    targets: frozenset[str]

    @classmethod
    def of(cls, *, user: str | None, session: str | None) -> "Owner":
        targets: set[str] = set()
        if user is not None:
            targets.add(f"user:{user}")
        if session is not None:
            # Close events are stored, so they carry a digest instead of the session key.
            targets.add(f"session:{hashlib.sha256(session.encode()).hexdigest()}")
        return cls(frozenset(targets))


def format_event(event: Message) -> str:
    """The event as an SSE message. Each line of the html gets its own data field."""
    lines = [f"id: {event.id}"]
    if event.event:
        lines.append(f"event: {event.event}")
    lines += [f"data: {line}" for line in LINE_BREAK.split(event.html)]
    return "\n".join(lines) + "\n\n"


def notify_payload(event_id: int, topic: str) -> str:
    # NOTIFY payloads are limited to 8000 bytes, so the html stays in the table.
    # The topic lets a process without subscribers skip loading the event.
    return f"{event_id}:{topic}"


def parse_payload(payload: str) -> tuple[int, str] | None:
    """The id and topic of a NOTIFY from notify_payload(), or None for any other payload."""
    head, _, topic = payload.partition(":")
    event_id = parse_last_event_id(head)
    return None if event_id is None else (event_id, topic)


def parse_last_event_id(value: str) -> int | None:
    # isdigit() alone accepts characters such as "²", which int() rejects.
    return int(value) if value.isascii() and value.isdigit() else None
