"""The listener of one event loop, which fans stored events out to that loop's open streams."""

import asyncio
import contextlib
import logging
import weakref
from collections.abc import AsyncIterator
from typing import Any

import psycopg
from django.db import connection
from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from django_eventstream_pg import conf
from django_eventstream_pg.messages import CHANNEL, CLOSE_PREFIX, Message, Owner, Signal, parse_payload
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


type Connection = psycopg.AsyncConnection[tuple[Any, ...]]


async def _connect() -> Connection:
    conn = await psycopg.AsyncConnection.connect(**connect_params(), autocommit=True)
    await _assume_role(conn)
    return conn


async def _assume_role(conn: Connection) -> None:
    # get_connection_params() leaves assume_role to Django's own connection setup.
    if role := connection.settings_dict["OPTIONS"].get("assume_role"):
        await conn.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role)))


class _Connection:
    """A connection outside Django's pool for one query at a time. It reconnects when broken."""

    def __init__(self) -> None:
        self.connection: Connection | None = None
        self.lock = asyncio.Lock()

    async def fetch(self, query: sql.SQL | sql.Composed, params: list[Any]) -> list[tuple[Any, ...]]:
        async with self.lock:
            # closed is also true for a broken connection.
            if self.connection is None or self.connection.closed:
                self.connection = await _connect()
            cursor = await self.connection.execute(query, params)
            return await cursor.fetchall()

    async def close(self) -> None:
        connection, self.connection = self.connection, None
        if connection is not None:
            await connection.close()


class _Pool:
    """Up to STREAM_CONNECTIONS connections outside Django's pool, opened on first use."""

    def __init__(self) -> None:
        self.pool: AsyncConnectionPool[Connection] | None = None
        self.lock = asyncio.Lock()
        self.closed = False

    @contextlib.asynccontextmanager
    async def connection(self) -> AsyncIterator[Connection]:
        async with (await self._open()).connection() as conn:
            yield conn

    async def _open(self) -> AsyncConnectionPool[Connection]:
        if (pool := self.pool) is not None:
            return pool
        async with self.lock:
            # A stream may outlive Hub.stop(); a new pool would stay open with nothing to close it.
            if self.closed:
                raise RuntimeError("The hub has stopped.")
            if (pool := self.pool) is None:
                pool = AsyncConnectionPool[Connection](
                    kwargs={**connect_params(), "autocommit": True},
                    min_size=1,
                    max_size=conf.get("STREAM_CONNECTIONS"),
                    configure=_assume_role,
                    name="eventstream_pg.streams",
                    open=False,
                )
                await pool.open()
                self.pool = pool
            return pool

    async def fetch(self, query: sql.SQL | sql.Composed, params: list[Any]) -> list[tuple[Any, ...]]:
        async with self.connection() as conn:
            cursor = await conn.execute(query, params)
            return await cursor.fetchall()

    async def close(self) -> None:
        self.closed = True
        pool, self.pool = self.pool, None
        if pool is not None:
            await pool.close()


class Hub:
    """Fans NOTIFY out to the open streams of one event loop, which is one ASGI worker.

    It holds connections of its own, outside Django's pool.
    The listener's connection waits on LISTEN.
    `live` loads the events the hub delivers.
    `streams` pools up to STREAM_CONNECTIONS connections for the queries of opening streams.
    An open stream therefore holds no pooled connection.
    """

    def __init__(self) -> None:
        self.queues: dict[str, set[Queue]] = {}
        # The topics of each queue, its owner's close topics included, so it can leave the index.
        self.subscriptions: dict[Queue, list[str]] = {}
        self.ready = asyncio.Event()
        self.last_id: int | None = None
        self.task: asyncio.Task[None] | None = None
        self.live = _Connection()
        # Replays run in parallel here, apart from live delivery.
        self.streams = _Pool()

    def subscribe(self, topics: list[str], owner: Owner) -> Queue:
        """A queue that receives the events of `topics` and the closes of `owner`.

        Starts the listener on first use.
        """
        queue: Queue = asyncio.Queue(QUEUE_SIZE)
        self.subscriptions[queue] = keys = [*topics, *owner.close_topics]
        for topic in keys:
            self.queues.setdefault(topic, set()).add(queue)
        if self.task is None or self.task.done():
            self.task = asyncio.get_running_loop().create_task(self._listen())
        return queue

    def unsubscribe(self, queue: Queue) -> None:
        for topic in self.subscriptions.pop(queue, ()):
            queues = self.queues.get(topic, set())
            queues.discard(queue)
            if not queues:
                self.queues.pop(topic, None)

    async def replay(self, topics: list[str], after_id: int) -> list[Message]:
        """Up to REPLAY_BATCH events of `topics` after `after_id`, oldest first."""
        return await _replay(self.streams, topics, after_id)

    async def any_since(self, topics: list[str], after_id: int) -> bool:
        """Whether one of `topics` has an event after `after_id`."""
        query = _select("WHERE topic = ANY(%s) AND id > %s LIMIT 1")
        return bool(await self.streams.fetch(query, [topics, after_id]))

    async def stop(self) -> None:
        # The listener task references the loop, so the weak key would never be collected.
        loop = asyncio.get_running_loop()
        if _hubs.get(loop) is self:
            del _hubs[loop]
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task
        try:
            await self.live.close()
        finally:
            await self.streams.close()

    async def _listen(self) -> None:
        delays = RECONNECT_DELAYS
        while True:
            try:
                async with await _connect() as listening:
                    await listening.execute(sql.SQL("LISTEN {}").format(sql.Identifier(CHANNEL)))
                    await self._catch_up()
                    self.ready.set()
                    delays = RECONNECT_DELAYS
                    while True:
                        # One query then loads the events of every NOTIFY received so far.
                        payloads = [notify.payload async for notify in listening.notifies(stop_after=1)]
                        payloads += [notify.payload async for notify in listening.notifies(timeout=0)]
                        await self._receive(payloads)
            except psycopg.Error:
                logger.warning("The SSE listener lost its connection; reconnecting.", exc_info=True)
            except Exception:
                # Open streams depend on this task; nothing else would restart it.
                logger.exception("The SSE listener failed; restarting.")
            self.ready.clear()
            await asyncio.sleep(delays[0])
            delays = delays[1:] or delays

    async def _receive(self, payloads: list[str]) -> None:
        events: list[tuple[int, str]] = []
        for payload in payloads:
            if (parsed := parse_payload(payload)) is None:
                logger.warning("Ignored a NOTIFY on %s with the payload %r.", CHANNEL, payload)
            else:
                events.append(parsed)
        # A close carries nothing beyond its topic, which the payload names.
        loaded = {
            event_id: Message(event_id, topic, "", "") for event_id, topic in events if topic.startswith(CLOSE_PREFIX)
        }
        requested = set(loaded)
        # A stream may subscribe while a query runs, so each query is followed by another check.
        while wanted := [event_id for event_id, topic in events if topic in self.queues and event_id not in requested]:
            requested.update(wanted)
            rows = await self.live.fetch(_select("WHERE id = ANY(%s)"), [wanted])
            loaded |= {row[0]: Message(*row) for row in rows}
        # No await from here on, so the subscriptions stay as checked.
        for event_id in sorted(loaded):
            self._deliver(loaded[event_id])
        # Unread ids advance only now, so _deliver skips none of the read ones before them.
        # They still count for the catch-up after a reconnect.
        if events:
            self.last_id = max(self.last_id or 0, max(event_id for event_id, _ in events))

    async def _catch_up(self) -> None:
        # LISTEN is active now, so events committed while it was not are in the table.
        if self.last_id is None:
            query = sql.SQL("SELECT coalesce(max(id), 0) FROM {}").format(_table())
            rows = await self.live.fetch(query, [])
            self.last_id = rows[0][0]
            return
        while True:
            messages = await _replay(self.live, list(self.queues), self.last_id)
            for message in messages:
                self._deliver(message)
            if len(messages) < REPLAY_BATCH:
                return

    def _deliver(self, message: Message) -> None:
        # Ids become visible in order, so an id at or below last_id was delivered before.
        if self.last_id is not None and message.id <= self.last_id:
            return
        self.last_id = message.id
        queues = list(self.queues.get(message.topic, ()))
        if message.topic.startswith(CLOSE_PREFIX):
            for queue in queues:
                self._end(queue, Signal.REVOKED)
            return
        for queue in queues:
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


async def _replay(connection: _Connection | _Pool, topics: list[str], after_id: int) -> list[Message]:
    query = _select("WHERE topic = ANY(%s) AND id > %s ORDER BY id LIMIT %s")
    rows = await connection.fetch(query, [topics, after_id, REPLAY_BATCH])
    return [Message(*row) for row in rows]


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
