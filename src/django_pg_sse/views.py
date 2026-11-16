"""The SSE endpoint: one stream per page, for several topics."""

import asyncio
import contextlib
import uuid
from collections.abc import AsyncGenerator, AsyncIterator

from asgiref.sync import sync_to_async
from django.db import connection, transaction
from django.db.models import Max
from django.dispatch import Signal as DjangoSignal
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
    digest,
    format_event,
)
from django_pg_sse.models import StreamEvent
from django_pg_sse.signals import StreamInfo, stream_closed, stream_opened

KEEPALIVE_SECONDS = 15
# A stream whose hub has not listened for this long ends, so the browser reconnects.
READY_TIMEOUT = 60


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
    owner, authorized_at, ids, info = authorized
    # An unsigned or foreign id resumes nothing; the stream starts with live events.
    last_event_id = ids.unsign(request.headers.get("Last-Event-ID", ""))
    messages = _with_keepalives(_messages(hub, owner, authorized_at, ids, last_event_id, info))
    response = StreamingHttpResponse(messages, content_type="text/event-stream")
    add_never_cache_headers(response)
    # nginx buffers proxied responses unless the response opts out.
    response["X-Accel-Buffering"] = "no"
    return response


def _authorize(
    request: HttpRequest, topics: list[str], known: int | None
) -> tuple[Owner, int, EventIds, StreamInfo] | None:
    """The stream's owner, the newest event id before the check, its id signer and its signal info.

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
    user_pk = session = None
    if request.user.is_authenticated:
        # Token authentication sets request.user without a session.
        request_session = getattr(request, "session", None)
        session_key = request_session.session_key if request_session is not None else None
        user_pk = str(request.user.pk)
        owner = Owner.of(user=user_pk, session=session_key)
        session = digest(session_key) if session_key is not None else None
        authorized_at = known or 0
    else:
        owner, authorized_at = Owner(()), 0
    info = StreamInfo(id=uuid.uuid4().hex, user_pk=user_pk, session=session, topics=tuple(topics))
    return owner, authorized_at, EventIds(owner, topics), info


async def _messages(  # noqa: PLR0913
    hub: Hub,
    owner: Owner,
    authorized_at: int,
    ids: EventIds,
    last_event_id: int | None,
    info: StreamInfo,
) -> AsyncGenerator[str, None]:
    await sync_to_async(_close_connection)()
    # Subscribed here, not in the view: a generator that never starts skips its finally.
    topics = list(info.topics)
    queue = hub.subscribe(topics, owner)
    sent = 0
    opened = False
    try:
        try:
            async with asyncio.timeout(READY_TIMEOUT):
                await hub.ready.wait()
        except TimeoutError:
            # The browser reconnects, perhaps to a worker whose listener runs.
            return
        # After a database restore, the browser's id can lie beyond the newest event.
        # An id this worker has not seen yet may come from another worker, so the table decides.
        if last_event_id is not None and last_event_id > (hub.last_id or 0) and last_event_id > await hub.newest():
            last_event_id = None
        sent = last_event_id or 0
        # Set first: a disconnect during the receivers must still send stream_closed.
        opened = True
        await _send(stream_opened, info)
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
            item = await queue.get()
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
        # Only a stream that began stream_opened sends stream_closed, so the two come in pairs.
        # shield: a second cancellation, such as at shutdown, must not cut the receivers short.
        if opened:
            await asyncio.shield(_send(stream_closed, info))


async def _send(signal: DjangoSignal, info: StreamInfo) -> None:
    if not signal.has_listeners(StreamInfo):
        return
    # asend_robust() logs a failing receiver to django.dispatch, and the stream goes on.
    await signal.asend_robust(sender=StreamInfo, stream=info)
    # A sync receiver ran in the request's thread and may have opened its connection again.
    await sync_to_async(_close_connection)()


async def _with_keepalives(messages: AsyncGenerator[str, None]) -> AsyncIterator[str]:
    """`messages` with a KEEPALIVE comment after every KEEPALIVE_SECONDS without output.

    A proxy idle timeout would otherwise cut a stream, whatever the stream waits for.
    """
    pending: asyncio.Future[str] | None = None
    try:
        while True:
            pending = pending or asyncio.ensure_future(anext(messages))
            done, _ = await asyncio.wait({pending}, timeout=KEEPALIVE_SECONDS)
            if not done:
                yield KEEPALIVE
                continue
            finished, pending = pending, None
            try:
                item = finished.result()
            except StopAsyncIteration:
                return
            yield item
    finally:
        # A disconnect cancels this generator; the pending step then ends with the same cancellation.
        if pending is not None:
            pending.cancel()
            with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                await pending
        await messages.aclose()


async def _revoked_since(hub: Hub, owner: Owner, event_id: int) -> bool:
    # Anonymous streams have no close topics, so no close can name them.
    return bool(owner.close_topics) and await hub.any_since(list(owner.close_topics), event_id)


def _close_connection() -> None:
    # Under ASGI the request's thread keeps its connection until the client leaves.
    # Closing it after the response middleware returns it to the pool.
    # The proxy resolves here, in that thread, not in the event loop's.
    connection.close()
