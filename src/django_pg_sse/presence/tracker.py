"""Presence bookkeeping of one event loop: its tracked streams, their heartbeat and the sweeps.

Every query runs on the hub's stream pool or the PG_SSE["DATABASE"] alias, outside Django's router.
Changes for one topic and identity hold an advisory lock, so two workers never both report one join or leave.
"""

import asyncio
import logging
import weakref
from typing import Any, cast

from asgiref.sync import sync_to_async
from django.db.models import BooleanField
from django.db.models.expressions import RawSQL
from django.utils.module_loading import import_string
from psycopg import sql
from psycopg.types.json import Jsonb

from django_pg_sse import conf
from django_pg_sse.hub import Connection, Hub, _fetch, _Pool, get_hub, hub_database
from django_pg_sse.models import TOPIC_MAX_LENGTH
from django_pg_sse.presence import Presence, presence_changed
from django_pg_sse.presence.models import PresenceEntry
from django_pg_sse.signals import StreamInfo
from django_pg_sse.views import _close_connection

logger = logging.getLogger(__name__)

# Seconds between two heartbeats of a worker; each also sweeps.
HEARTBEAT_SECONDS = 15
# A row whose heartbeat stopped this long ago belongs to a killed worker.
EXPIRE_SECONDS = 45
# A closed stream's row stays this long, so a reconnect sends no leave and join.
GRACE_SECONDS = 5

type Pair = tuple[str, str]
# stream, topic, identity, meta
type Row = tuple[str, str, str, dict[str, Any]]

# Rows a sweep deletes and present() skips; the parameters are GRACE_SECONDS and EXPIRE_SECONDS.
# IS NOT NULL keeps the condition false rather than NULL for open streams, so its negation works.
GONE = (
    "((closed_at IS NOT NULL AND closed_at < now() - make_interval(secs => %s)) "
    "OR last_seen < now() - make_interval(secs => %s))"
)
# The rows of a list of pairs, given as two arrays.
PAIRS = "(topic, identity) IN (SELECT * FROM unnest(%s::text[], %s::text[]))"


class Tracker:
    """The streams of one event loop that presence tracks, and the tasks that keep them current."""

    def __init__(self) -> None:
        # Bound by the first join: an open stream has a running hub.
        self.hub: Hub | None = None
        # The rows of each tracked stream, for the heartbeat.
        self.streams: dict[str, list[Row]] = {}
        # Streams that closed since the last batch of closes went out.
        self.closing: set[str] = set()
        self.heartbeat: asyncio.Task[None] | None = None
        self.closer: asyncio.Task[None] | None = None
        self.sweeper: asyncio.Task[None] | None = None
        self.sweep_due = False

    async def join(self, info: StreamInfo) -> None:
        """Track `info`'s presence topics and report the identity on each topic it was absent from."""
        topics = sorted({topic for topic in info.topics if _tracked(topic)})
        if not topics:
            return
        identity_function = import_string(conf.get("PRESENCE_IDENTITY"))
        # The identity function may read the session, which loads from the database.
        # views._send closes the request thread's connection after this receiver.
        found = await sync_to_async(identity_function)(info.request)
        if found is None:
            return
        identity, meta = found
        pool = self._pool(start=True)
        assert pool is not None
        rows = [(info.id, topic, identity, meta) for topic in topics]
        # Registered first: a cancellation after the commit must still let leave() close the rows.
        self.streams[info.id] = rows
        if self.heartbeat is None or self.heartbeat.done():
            self.heartbeat = asyncio.get_running_loop().create_task(self._beat())
        await _report(await _write(pool, rows), joined=True)

    async def leave(self, info: StreamInfo) -> None:
        """Mark `info`'s rows closed; a sweep after the grace period reports the leave."""
        if self.streams.pop(info.id, None) is None:
            return
        # One batch closes every stream that closed until it runs, such as after a deploy.
        self.closing.add(info.id)
        if self.closer is None or self.closer.done():
            self.closer = asyncio.get_running_loop().create_task(self._close())

    async def sweep(self, *, start: bool = True) -> None:
        """Delete closed rows past the grace period and expired ones; report identities without rows left.

        `start` binds a tracker without streams to the loop's hub, such as in a test.
        """
        if (pool := self._pool(start=start)) is None:
            return
        limits = [GRACE_SECONDS, EXPIRE_SECONDS]
        async with pool.connection() as conn, conn.transaction():
            found = await _fetch(conn, _query(f"SELECT DISTINCT topic, identity FROM {{}} WHERE {GONE}"), limits)
            pairs = cast(list[Pair], found)
            if not pairs:
                return
            await _lock(conn, pairs)
            # A sweep that waited for the lock finds the rows another sweep already deleted gone.
            deleted = await _fetch(
                conn,
                _query(f"DELETE FROM {{}} WHERE {GONE} AND {PAIRS} RETURNING topic, identity, meta"),
                [*limits, *_columns(pairs)],
            )
            still_present = await _present_pairs(conn, pairs)
        left = {(topic, identity): meta for topic, identity, meta in deleted if (topic, identity) not in still_present}
        await _report(left, joined=False)

    async def stop(self) -> None:
        """Cancel the heartbeat and pending closes, such as before the hub stops."""
        tasks = [task for task in (self.heartbeat, self.closer, self.sweeper) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.streams.clear()
        self.closing.clear()
        loop = asyncio.get_running_loop()
        if _trackers.get(loop) is self:
            del _trackers[loop]

    def _pool(self, *, start: bool) -> _Pool | None:
        # A stopped hub's pool refuses to reopen; a new one would stay open with nothing to close it.
        if self.hub is None or self.hub.streams.closed:
            if not start:
                return None
            self.hub = get_hub()
        return self.hub.streams

    async def _close(self) -> None:
        while self.closing:
            # One loop turn lets closes of the same moment join this batch.
            await asyncio.sleep(0)
            streams, self.closing = list(self.closing), set()
            if (pool := self._pool(start=False)) is None:
                return
            try:
                await pool.fetch(
                    _query("UPDATE {} SET closed_at = now() WHERE stream = ANY(%s) RETURNING id"), [streams]
                )
            except Exception:
                # The rows expire without a heartbeat, and a later sweep reports the leave.
                logger.warning("Marking closed presence streams failed.", exc_info=True)
                continue
            # One pending sweep covers every close until it runs.
            self.sweep_due = True
            if self.sweeper is None or self.sweeper.done():
                self.sweeper = asyncio.get_running_loop().create_task(self._sweep_soon())

    async def _sweep_soon(self) -> None:
        while self.sweep_due:
            self.sweep_due = False
            await asyncio.sleep(GRACE_SECONDS)
            await self._sweep_logged()

    async def _beat(self) -> None:
        while self.streams:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            if (pool := self._pool(start=False)) is None:
                return
            try:
                await self._refresh(pool)
            except Exception:
                logger.warning("The presence heartbeat failed; retrying.", exc_info=True)
                continue
            await self._sweep_logged()

    async def _refresh(self, pool: _Pool) -> None:
        # A plain update without locks for the rows that exist, which is all of them as a rule.
        refreshed = await pool.fetch(
            _query(
                "UPDATE {} SET last_seen = now() WHERE stream = ANY(%s) AND closed_at IS NULL RETURNING stream, topic"
            ),
            [list(self.streams)],
        )
        found = {(stream, topic) for stream, topic in refreshed}
        # A crash or another worker's sweep removed the rest; they come back, and their readers join again.
        missing = [row for rows in self.streams.values() for row in rows if (row[0], row[1]) not in found]
        await _report(await _write(pool, missing), joined=True)

    async def _sweep_logged(self) -> None:
        # A failed sweep leaves its rows for the next one.
        try:
            await self.sweep(start=False)
        except Exception:
            logger.warning("The presence sweep failed; the next one retries.", exc_info=True)


async def _write(pool: _Pool, rows: list[Row]) -> dict[Pair, dict[str, Any]]:
    """Insert or refresh `rows`; return the identities that had no row on their topic before."""
    if not rows:
        return {}
    pairs = sorted({(topic, identity) for _, topic, identity, _ in rows})
    async with pool.connection() as conn, conn.transaction():
        await _lock(conn, pairs)
        # Rows in their grace period count: the identity is still present.
        present_before = await _present_pairs(conn, pairs)
        streams, topics, identities, metas = (list(column) for column in zip(*rows, strict=True))
        await _fetch(
            conn,
            _query(
                "INSERT INTO {} (stream, topic, identity, meta, last_seen) "
                "SELECT stream, topic, identity, meta, now() "
                "FROM unnest(%s::text[], %s::text[], %s::text[], %s::jsonb[]) AS row(stream, topic, identity, meta) "
                "ON CONFLICT (stream, topic) DO UPDATE SET last_seen = excluded.last_seen RETURNING id"
            ),
            [streams, topics, identities, [Jsonb(meta) for meta in metas]],
        )
    return {(topic, identity): meta for _, topic, identity, meta in rows if (topic, identity) not in present_before}


async def _present_pairs(conn: Connection, pairs: list[Pair]) -> set[Pair]:
    """Those of `pairs` with at least one row."""
    rows = await _fetch(conn, _query(f"SELECT DISTINCT topic, identity FROM {{}} WHERE {PAIRS}"), _columns(pairs))
    return {(topic, identity) for topic, identity in rows}


def _tracked(topic: str) -> bool:
    # A longer topic cannot be stored, and broadcast() refuses it anyway.
    return len(topic) <= TOPIC_MAX_LENGTH and topic.startswith(tuple(conf.get("PRESENCE_TOPICS")))


def _columns(pairs: list[Pair]) -> list[list[str]]:
    return [[topic for topic, _ in pairs], [identity for _, identity in pairs]]


async def _lock(conn: Connection, pairs: list[Pair]) -> None:
    # One statement, in the order of the lock keys: string order may differ when two hashes collide.
    await _fetch(
        conn,
        sql.SQL(
            "SELECT pg_advisory_xact_lock(a, b) FROM (SELECT DISTINCT hashtext(topic) AS a, hashtext(identity) AS b "
            "FROM unnest(%s::text[], %s::text[]) AS pair(topic, identity) ORDER BY a, b) AS keys"
        ),
        _columns(pairs),
    )


async def _report(changes: dict[Pair, dict[str, Any]], *, joined: bool) -> None:
    by_topic: dict[str, list[Presence]] = {}
    for (topic, identity), meta in changes.items():
        by_topic.setdefault(topic, []).append(Presence(identity, meta))
    for topic, entries in sorted(by_topic.items()):
        changed = tuple(entries)
        await presence_changed.asend_robust(
            sender=Presence, topic=topic, joined=changed if joined else (), left=() if joined else changed
        )
    if by_topic:
        # A sync receiver ran in a thread of its own and may have opened a connection there.
        await sync_to_async(_close_connection)()


def _query(text: str) -> sql.Composed:
    # text is a literal from this module, never user input.
    return sql.SQL(text).format(sql.Identifier(PresenceEntry._meta.db_table))  # pyright: ignore[reportArgumentType]


def present(topic: str) -> list[Presence]:
    """The readers of `topic`, each identity once, in the order they joined.

    Expired rows and rows closed past the grace period do not count, even before a sweep deletes them.
    """
    gone = RawSQL(GONE, [GRACE_SECONDS, EXPIRE_SECONDS], output_field=BooleanField())
    # The hub's alias: a router could send the read to a standby, which refuses UNLOGGED tables.
    entries = PresenceEntry.objects.using(hub_database().alias).filter(topic=topic).exclude(gone).order_by("id")
    readers: dict[str, Presence] = {}
    for identity, meta in entries.values_list("identity", "meta"):
        readers.setdefault(identity, Presence(identity, meta))
    return list(readers.values())


async def apresent(topic: str) -> list[Presence]:
    """present() for async code."""
    return await sync_to_async(present)(topic)


async def stream_opened_receiver(sender: Any, stream: StreamInfo, **kwargs: Any) -> None:  # noqa: ARG001
    """Connected to stream_opened by the app config."""
    await get_tracker().join(stream)


async def stream_closed_receiver(sender: Any, stream: StreamInfo, **kwargs: Any) -> None:  # noqa: ARG001
    """Connected to stream_closed by the app config."""
    await get_tracker().leave(stream)


_trackers: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Tracker] = weakref.WeakKeyDictionary()


def get_tracker() -> Tracker:
    """The tracker of the running event loop."""
    loop = asyncio.get_running_loop()
    if loop not in _trackers:
        _trackers[loop] = Tracker()
    return _trackers[loop]
