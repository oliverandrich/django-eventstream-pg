"""SSE message format, NOTIFY payloads and the values streams exchange with the hub."""

import hashlib
import json
import re
from dataclasses import dataclass
from enum import Enum
from functools import cached_property, lru_cache

from django.core import signing

# The PostgreSQL channel that carries "<id>:<topic>" for every stored event.
CHANNEL = "pg_sse"
# Topics with this prefix belong to the package. The view refuses them before CAN_SUBSCRIBE runs.
RESERVED_PREFIX = "_"
# close_streams() stores an event on each close topic of the owner, so it replays like any event.
CLOSE_PREFIX = RESERVED_PREFIX + "close:"

# SSE ends a line at CRLF, CR or LF, so each of them must start a new data field.
LINE_BREAK = re.compile(r"\r\n|\r|\n")

CONNECTED = ": connected\n\n"
KEEPALIVE = ": keepalive\n\n"
CLOSED_EVENT = "closed"
CLOSED = f"event: {CLOSED_EVENT}\ndata: \n\n"
# broadcast() refuses these names. hx-sse:close="closed" ends a stream, and htmx 4 releases its
# connection on hx:release.
RESERVED_EVENTS = frozenset({CLOSED_EVENT, "hx:release"})


@dataclass(frozen=True)
class Message:
    id: int
    topic: str
    event: str
    data: str

    @cached_property
    def body(self) -> str:
        """The SSE message without its id line, formatted once for every stream."""
        lines = [f"event: {self.event}"] if self.event else []
        lines += [f"data: {line}" for line in LINE_BREAK.split(self.data)]
        return "\n".join(lines) + "\n\n"


class Signal(Enum):
    # The client fell too far behind. The stream ends, and htmx reconnects with Last-Event-ID,
    # so the replay delivers what the queue dropped.
    OVERFLOW = "overflow"
    # close_streams() named the stream. It sends CLOSED and ends.
    REVOKED = "revoked"


@dataclass(frozen=True)
class Owner:
    """The topics on which close_streams() ends a stream of this user or session, sorted."""

    close_topics: tuple[str, ...]

    @classmethod
    def of(cls, *, user: str | None, session: str | None) -> "Owner":
        keys = {kind: key for kind, key in (("user", user), ("session", session)) if key is not None}
        # A digest keeps the session key out of the table and fits any primary key into the column.
        topics = (f"{CLOSE_PREFIX}{kind}:{hashlib.sha256(key.encode()).hexdigest()}" for kind, key in keys.items())
        return cls(tuple(sorted(topics)))


class EventIds:
    """Signs the event ids of one stream for its owner and topics.

    A Last-Event-ID then resumes only an id that a stream of the same user, session and topics
    received. An unsigned or foreign id resumes nothing.
    """

    def __init__(self, owner: Owner, topics: list[str]) -> None:
        self.salt = "django_pg_sse.ids:" + json.dumps([owner.close_topics, sorted(set(topics))])
        self.signer = signing.Signer(salt=self.salt)

    def sign(self, event_id: int) -> str:
        return _sign(self.salt, event_id)

    def unsign(self, value: str) -> int | None:
        try:
            return parse_last_event_id(self.signer.unsign(value))
        except signing.BadSignature:
            return None


# Streams with the same owner and topics, such as anonymous readers of a public topic, share one
# signature per event. The derived key depends on SECRET_KEY, which a running process keeps.
@lru_cache(maxsize=4096)
def _sign(salt: str, event_id: int) -> str:
    return signing.Signer(salt=salt).sign(str(event_id))


def format_event(event: Message, event_id: str) -> str:
    """The event as an SSE message: the id line, then the formatted body."""
    return f"id: {event_id}\n{event.body}"


def notify_payload(event_id: int, topic: str) -> str:
    # NOTIFY payloads are limited to 8000 bytes, so the data stays in the table.
    # The topic lets a process without subscribers skip loading the event.
    # publish._publish_sql() builds the same payload in SQL.
    return f"{event_id}:{topic}"


def parse_payload(payload: str) -> tuple[int, str] | None:
    """The id and topic of a NOTIFY from notify_payload(), or None for any other payload."""
    head, _, topic = payload.partition(":")
    event_id = parse_last_event_id(head)
    return None if event_id is None else (event_id, topic)


def parse_last_event_id(value: str) -> int | None:
    # isdigit() alone accepts characters such as "²", which int() rejects.
    return int(value) if value.isascii() and value.isdigit() else None
