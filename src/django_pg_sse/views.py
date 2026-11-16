"""The SSE endpoint: one stream per page, for several topics."""

import asyncio
from collections.abc import AsyncIterator

from asgiref.sync import sync_to_async
from django.db import connection, transaction
from django.db.models import Max
from django.http import HttpRequest, HttpResponseBadRequest, HttpResponseForbidden, StreamingHttpResponse
from django.http.response import HttpResponseBase
from django.utils.cache import add_never_cache_headers

from django_pg_sse import conf
from django_pg_sse import hub as hub_module
from django_pg_sse.hub import Hub, get_hub
from django_pg_sse.messages import (
    CLOSED,
    CONNECTED,
    KEEPALIVE,
    RESERVED_PREFIX,
    EventIds,
    Owner,
    Signal,
    format_event,
)
from django_pg_sse.models import StreamEvent

KEEPALIVE_SECONDS = 15


# Django raises for an async view under ATOMIC_REQUESTS, and a stream cannot hold a transaction.
@transaction.non_atomic_requests
async def stream(request: HttpRequest) -> HttpResponseBase:
    """`?topic=a&topic=b`: one SSE stream for several topics, so a page needs one connection."""
    topics = request.GET.getlist("topic")
    if not topics:
        return HttpResponseBadRequest("Name at least one topic.")
    # Reserved topics carry the package's own events, such as closes; no CAN_SUBSCRIBE may open them.
    if any(topic.startswith(RESERVED_PREFIX) for topic in topics):
        return HttpResponseForbidden()
    hub = get_hub()
    # Read before the check: a close committed after it has a larger id and is caught later.
    known = hub.last_id if hub.ready.is_set() else None
    authorized = await sync_to_async(_authorize)(request, topics, known)
    if authorized is None:
        return HttpResponseForbidden()
    owner, authorized_at, ids = authorized
    # An unsigned or foreign id resumes nothing; the stream starts with live events.
    last_event_id = ids.unsign(request.headers.get("Last-Event-ID", ""))
    messages = _messages(hub, topics, owner, authorized_at, ids, last_event_id)
    response = StreamingHttpResponse(messages, content_type="text/event-stream")
    add_never_cache_headers(response)
    return response


def _authorize(request: HttpRequest, topics: list[str], known: int | None) -> tuple[Owner, int, EventIds] | None:
    """The stream's owner, the newest event id before the check and its id signer.

    None if a topic is not allowed.

    `known` is the hub's newest id. A hub that is not listening yet needs one query instead.
    """
    # An authenticated stream reads the newest id before the check, since a close may follow it.
    if request.user.is_authenticated and known is None:
        known = StreamEvent.objects.aggregate(newest=Max("id"))["newest"] or 0
    can_subscribe = conf.can_subscribe()
    if not all(can_subscribe(request, topic) for topic in topics):
        return None
    # Anonymous streams have no close topics: close_streams() needs a user or a session login.
    if request.user.is_authenticated:
        owner = Owner.of(user=str(request.user.pk), session=request.session.session_key)
        authorized_at = known or 0
    else:
        owner, authorized_at = Owner(()), 0
    return owner, authorized_at, EventIds(owner, topics)


async def _messages(
    hub: Hub, topics: list[str], owner: Owner, authorized_at: int, ids: EventIds, last_event_id: int | None
) -> AsyncIterator[str]:
    await sync_to_async(_close_connection)()
    # Subscribed here, not in the view: a generator that never starts skips its finally.
    queue = hub.subscribe(topics, owner)
    sent = last_event_id or 0
    try:
        await hub.ready.wait()
        yield CONNECTED
        # A close among the events since authorisation reached the hub before this queue existed.
        missed = (hub.last_id or 0) > authorized_at
        if missed and await _revoked_since(hub, owner, authorized_at):
            yield CLOSED
            return
        # A close after the id took access away in between, so those events stay unread.
        if last_event_id is not None and not await _revoked_since(hub, owner, last_event_id):
            replayed = await hub.replay(topics, sent)
            for message in replayed:
                sent = message.id
                yield format_event(message, ids.sign(message.id))
            if len(replayed) == hub_module.REPLAY_BATCH:
                return
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), KEEPALIVE_SECONDS)
            except TimeoutError:
                yield KEEPALIVE
                continue
            if item is Signal.OVERFLOW:
                return
            if item is Signal.REVOKED:
                yield CLOSED
                return
            assert not isinstance(item, Signal)
            # The replay may already have sent what the queue collected meanwhile.
            if item.id <= sent:
                continue
            sent = item.id
            yield format_event(item, ids.sign(item.id))
    finally:
        hub.unsubscribe(queue)


async def _revoked_since(hub: Hub, owner: Owner, event_id: int) -> bool:
    # Anonymous streams have no close topics, so no close can name them.
    return bool(owner.close_topics) and await hub.any_since(list(owner.close_topics), event_id)


def _close_connection() -> None:
    # Under ASGI the request's thread keeps its connection until the client leaves.
    # Closing it after the response middleware returns it to the pool.
    # The proxy resolves here, in that thread, not in the event loop's.
    connection.close()
