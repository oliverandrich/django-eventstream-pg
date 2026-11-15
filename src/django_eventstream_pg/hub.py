"""The listener of one event loop, which fans stored events out to that loop's open streams."""

import asyncio
import contextlib
import logging
import weakref
from typing import Any

import psycopg
from django.db import connection
from psycopg import sql

from django_eventstream_pg.messages import CHANNEL, CLOSE_TOPIC, Message, Owner, Signal, parse_payload
from django_eventstream_pg.models import StreamEvent

logger = logging.getLogger(__name__)

QUEUE_SIZE = 1000
# A longer replay ends the stream after one batch; htmx reconnects and asks for the next.
REPLAY_BATCH = 500
RECONNECT_DELAYS = (0.5, 1, 2, 5)

type Queue = asyncio.Queue[Message | Signal]


def connect_params() -> dict[str, Any]:
    """Arguments for a psycopg connection to the default database, outside Django's pool."""
    params: dict[str, Any] = connection.get_connection_params()
    # Django's sync cursor factory and adapter context do not fit an AsyncConnection.
    params.pop("cursor_factory", None)
    params.pop("context", None)
    return params


async def _connect() -> psycopg.AsyncConnection[tuple[Any, ...]]:
    conn = await psycopg.AsyncConnection.connect(**connect_params(), autocommit=True)
    # get_connection_params() leaves assume_role to Django's own connection setup.
    if role := connection.settings_dict["OPTIONS"].get("assume_role"):
        await conn.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role)))
    return conn


class Hub:
    """Fans NOTIFY out to the open streams of one event loop, which is one ASGI worker.

    It holds two connections of its own, outside Django's pool: one waits on LISTEN, the other
    loads events. An open stream therefore holds no pooled connection.
    """

    def __init__(self) -> None:
        self.queues: dict[str, set[Queue]] = {}
        self.by_target: dict[str, set[Queue]] = {}
        # The topics and targets of each queue, so a queue can leave both indexes at once.
        self.subscriptions: dict[Queue, tuple[list[str], frozenset[str]]] = {}
        self.ready = asyncio.Event()
        self.last_id: int | None = None
        self.task: asyncio.Task[None] | None = None
        self.fetch_connection: psycopg.AsyncConnection[tuple[Any, ...]] | None = None
        self.fetch_lock = asyncio.Lock()

    def subscribe(self, topics: list[str], owner: Owner) -> Queue:
        """A queue that receives the events of `topics`. Starts the listener on first use."""
        queue: Queue = asyncio.Queue(QUEUE_SIZE)
        self.subscriptions[queue] = (topics, owner.targets)
        for target in owner.targets:
            self.by_target.setdefault(target, set()).add(queue)
        for topic in topics:
            self.queues.setdefault(topic, set()).add(queue)
        if self.task is None or self.task.done():
            self.task = asyncio.get_running_loop().create_task(self._listen())
        return queue

    def unsubscribe(self, queue: Queue) -> None:
        if (subscription := self.subscriptions.pop(queue, None)) is None:
            return
        topics, targets = subscription
        _discard(self.by_target, targets, queue)
        _discard(self.queues, topics, queue)

    async def replay(self, topics: list[str], after_id: int) -> list[Message]:
        """Up to REPLAY_BATCH events of `topics` after `after_id`, oldest first."""
        query = _select("WHERE topic = ANY(%s) AND id > %s ORDER BY id LIMIT %s")
        rows = await self._fetch(query, [topics, after_id, REPLAY_BATCH])
        return [Message(*row) for row in rows]

    async def closed_since(self, targets: frozenset[str], after_id: int) -> bool:
        """Whether close_streams() named one of `targets` after `after_id`."""
        query = _select("WHERE topic = %s AND id > %s AND html = ANY(%s) LIMIT 1")
        return bool(await self._fetch(query, [CLOSE_TOPIC, after_id, list(targets)]))

    async def stop(self) -> None:
        # The listener task references the loop, so the weak key would never be collected.
        loop = asyncio.get_running_loop()
        if _hubs.get(loop) is self:
            del _hubs[loop]
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task
        if self.fetch_connection is not None:
            await self.fetch_connection.close()

    async def _fetch(self, query: sql.SQL | sql.Composed, params: list[Any]) -> list[tuple[Any, ...]]:
        async with self.fetch_lock:
            if self.fetch_connection is None or self.fetch_connection.broken:
                self.fetch_connection = await _connect()
            cursor = await self.fetch_connection.execute(query, params)
            return await cursor.fetchall()

    async def _listen(self) -> None:
        delays = RECONNECT_DELAYS
        while True:
            try:
                async with await _connect() as listening:
                    await listening.execute(sql.SQL("LISTEN {}").format(sql.Identifier(CHANNEL)))
                    await self._catch_up()
                    self.ready.set()
                    delays = RECONNECT_DELAYS
                    async for notify in listening.notifies():
                        await self._receive(notify.payload)
            except psycopg.Error:
                logger.warning("The SSE listener lost its connection; reconnecting.", exc_info=True)
            except Exception:
                # Open streams depend on this task; nothing else would restart it.
                logger.exception("The SSE listener failed; restarting.")
            self.ready.clear()
            await asyncio.sleep(delays[0])
            delays = delays[1:] or delays

    async def _receive(self, payload: str) -> None:
        parsed = parse_payload(payload)
        if parsed is None:
            logger.warning("Ignored a NOTIFY on %s with the payload %r.", CHANNEL, payload)
            return
        event_id, topic = parsed
        readers = self.by_target if topic == CLOSE_TOPIC else self.queues.get(topic)
        if not readers:
            # No stream here reads the topic; ids still advance for the catch-up after a reconnect.
            self.last_id = max(self.last_id or 0, event_id)
            return
        for row in await self._fetch(_select("WHERE id = %s"), [event_id]):
            self._deliver(Message(*row))

    async def _catch_up(self) -> None:
        # LISTEN is active now, so events committed while it was not are in the table.
        if self.last_id is None:
            query = sql.SQL("SELECT coalesce(max(id), 0) FROM {}").format(_table())
            rows = await self._fetch(query, [])
            self.last_id = rows[0][0]
            return
        while True:
            messages = await self.replay([*self.queues, CLOSE_TOPIC], self.last_id)
            for message in messages:
                self._deliver(message)
            if len(messages) < REPLAY_BATCH:
                return

    def _close(self, target: str) -> None:
        for queue in list(self.by_target.get(target, ())):
            self._end(queue, Signal.REVOKED)

    def _deliver(self, message: Message) -> None:
        # Ids become visible in order, so an id at or below last_id was delivered before.
        if self.last_id is not None and message.id <= self.last_id:
            return
        self.last_id = message.id
        if message.topic == CLOSE_TOPIC:
            self._close(message.html)
            return
        for queue in list(self.queues.get(message.topic, ())):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                self._end(queue, Signal.OVERFLOW)

    def _end(self, queue: Queue, signal: Signal) -> None:
        # An unsubscribed queue receives nothing more, so the signal stays its last item.
        self.unsubscribe(queue)
        while not queue.empty():
            queue.get_nowait()
        queue.put_nowait(signal)


def _discard(index: dict[str, set[Queue]], keys: list[str] | frozenset[str], queue: Queue) -> None:
    for key in keys:
        queues = index.get(key, set())
        queues.discard(queue)
        if not queues:
            index.pop(key, None)


def _table() -> sql.Identifier:
    return sql.Identifier(StreamEvent._meta.db_table)


def _select(condition: str) -> sql.Composed:
    # condition is a literal from this package, never user input.
    query = "SELECT id, topic, event, html FROM {} " + condition
    return sql.SQL(query).format(_table())  # pyright: ignore[reportArgumentType]


_hubs: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Hub] = weakref.WeakKeyDictionary()


def get_hub() -> Hub:
    """The hub of the running event loop. Each ASGI worker runs one loop."""
    loop = asyncio.get_running_loop()
    if loop not in _hubs:
        _hubs[loop] = Hub()
    return _hubs[loop]
