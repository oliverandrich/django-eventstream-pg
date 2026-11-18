"""Presence bookkeeping of one event loop: its tracked streams, their heartbeat and the sweeps.

Every query runs on the hub's stream pool or the PG_SSE["DATABASE"] alias, outside Django's router.
Changes for one topic and identity hold an advisory lock, so two workers never both report one join or leave.
"""

import asyncio
import logging
from typing import Any, Literal, cast

from asgiref.sync import sync_to_async
from django.db.models import BooleanField
from django.db.models.expressions import RawSQL
from django.utils.module_loading import import_string
from psycopg import sql
from psycopg.types.json import Jsonb

from django_pg_sse import conf, signals
from django_pg_sse.hub import Hub, StreamPool, Transaction, get_hub, hub_database
from django_pg_sse.models import TOPIC_MAX_LENGTH
from django_pg_sse.presence import Presence, presence_changed
from django_pg_sse.presence.models import PresenceEntry
from django_pg_sse.signals import StreamInfo

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
# The meta of each changed topic and identity.
type Changes = dict[Pair, dict[str, Any]]

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

    def __init__(self, hub: Hub) -> None:
        self.hub = hub
        # The hub stops its tracker before it closes the pool the tracker writes through.
        hub.on_stop.append(self.stop)
        # The rows of each tracked stream, for the heartbeat.
        self.streams: dict[str, list[Row]] = {}
        # Streams that closed since the last batch of closes went out.
        self.closing: set[str] = set()
        self.heartbeat: asyncio.Task[None] | None = None
        self.closer: asyncio.Task[None] | None = None
        self.sweeper: asyncio.Task[None] | None = None
        self.sweep_due = False
        self.stopped = False

    async def join(self, info: StreamInfo) -> None:
        """Track `info`'s presence topics and report the identity on each topic it was absent from."""
        topics = sorted({topic for topic in info.topics if _tracked(topic)})
        if not topics:
            return
        identity_function = import_string(conf.get("PRESENCE_IDENTITY"))
        # The identity function may read the session, which loads from the database.
        # signals.asend() closes the shared sync thread's connections after this receiver.
        found = await sync_to_async(identity_function)(info.request)
        # Hub.stop() may have stopped this tracker while the identity function ran.
        if found is None or self.stopped:
            return
        identity, meta = found
        rows = [(info.id, topic, identity, meta) for topic in topics]
        # Registered first: a cancellation after the commit must still let leave() close the rows.
        self.streams[info.id] = rows
        if self.heartbeat is None or self.heartbeat.done():
            self.heartbeat = asyncio.get_running_loop().create_task(self._beat())
        joined, updated = await _write(self.hub.streams, rows)
        await _report(joined, "joined")
        await _report(updated, "updated")

    async def leave(self, info: StreamInfo) -> None:
        """Mark `info`'s rows closed; a sweep after the grace period reports the leave."""
        if self.streams.pop(info.id, None) is None:
            return
        # One batch closes every stream that closed until it runs, such as after a deploy.
        self.closing.add(info.id)
        if self.closer is None or self.closer.done():
            self.closer = asyncio.get_running_loop().create_task(self._close())

    async def sweep(self) -> None:
        """Delete closed rows past the grace period and expired ones; report identities without rows left."""
        limits = [GRACE_SECONDS, EXPIRE_SECONDS]
        async with self.hub.streams.transaction() as tx:
            found = await tx.fetch(_query(f"SELECT DISTINCT topic, identity FROM {{}} WHERE {GONE}"), limits)
            pairs = cast(list[Pair], found)
            if not pairs:
                return
            await _lock(tx, pairs)
            # A sweep that waited for the lock finds the rows another sweep already deleted gone.
            deleted = await tx.fetch(
                _query(f"DELETE FROM {{}} WHERE {GONE} AND {PAIRS} RETURNING topic, identity, meta"),
                [*limits, *_columns(pairs)],
            )
            still_present = await _newest_metas(tx, pairs)
        left = {(topic, identity): meta for topic, identity, meta in deleted if (topic, identity) not in still_present}
        await _report(left, "left")

    async def stop(self) -> None:
        """Cancel the heartbeat and pending closes, and mark every stream of this tracker closed.

        Hub.stop() calls it before the pool closes. Other workers then report the leaves after the grace period.
        """
        self.stopped = True
        tasks = [task for task in (self.heartbeat, self.closer, self.sweeper) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        streams = [*self.streams, *self.closing]
        self.streams.clear()
        self.closing.clear()
        if streams:
            try:
                await self.hub.streams.fetch(_CLOSE, [streams])
            except Exception:
                # The rows expire without a heartbeat instead.
                logger.warning("Marking presence streams closed at stop failed.", exc_info=True)

    async def _close(self) -> None:
        while self.closing:
            # One loop turn lets closes of the same moment join this batch.
            await asyncio.sleep(0)
            streams, self.closing = list(self.closing), set()
            try:
                await self.hub.streams.fetch(_CLOSE, [streams])
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
            try:
                await self._refresh()
            except Exception:
                logger.warning("The presence heartbeat failed; retrying.", exc_info=True)
                continue
            await self._sweep_logged()

    async def _refresh(self) -> None:
        # A plain update without locks for the rows that exist, which is all of them as a rule.
        refreshed = await self.hub.streams.fetch(
            _query(
                "UPDATE {} SET last_seen = now() WHERE stream = ANY(%s) AND closed_at IS NULL RETURNING stream, topic"
            ),
            [list(self.streams)],
        )
        found = {(stream, topic) for stream, topic in refreshed}
        # A crash or another worker's sweep removed the rest; they come back, and their readers join again.
        missing = [row for rows in self.streams.values() for row in rows if (row[0], row[1]) not in found]
        joined, updated = await _write(self.hub.streams, missing)
        await _report(joined, "joined")
        await _report(updated, "updated")

    async def _sweep_logged(self) -> None:
        # A failed sweep leaves its rows for the next one.
        try:
            await self.sweep()
        except Exception:
            logger.warning("The presence sweep failed; the next one retries.", exc_info=True)


async def _write(pool: StreamPool, rows: list[Row]) -> tuple[Changes, Changes]:
    """Insert or refresh `rows`.

    Return the identities that had no row on their topic before, and those whose meta changed.
    """
    if not rows:
        return {}, {}
    pairs = sorted({(topic, identity) for _, topic, identity, _ in rows})
    async with pool.transaction() as tx:
        await _lock(tx, pairs)
        # Rows in their grace period count: the identity is still present.
        before = await _newest_metas(tx, pairs)
        streams, topics, identities, metas = (list(column) for column in zip(*rows, strict=True))
        await tx.fetch(
            _query(
                "INSERT INTO {} (stream, topic, identity, meta, last_seen) "
                "SELECT stream, topic, identity, meta, now() "
                "FROM unnest(%s::text[], %s::text[], %s::text[], %s::jsonb[]) AS row(stream, topic, identity, meta) "
                "ON CONFLICT (stream, topic) DO UPDATE SET last_seen = excluded.last_seen RETURNING id"
            ),
            [streams, topics, identities, [Jsonb(meta) for meta in metas]],
        )
    written = {(topic, identity): meta for _, topic, identity, meta in rows}
    joined = {pair: meta for pair, meta in written.items() if pair not in before}
    # A reader with a new meta, such as a new name, reconnects within the grace period: no join tells the others.
    updated = {pair: meta for pair, meta in written.items() if pair in before and before[pair] != meta}
    return joined, updated


async def _newest_metas(tx: Transaction, pairs: list[Pair]) -> dict[Pair, dict[str, Any]]:
    """The meta of the newest row of each of `pairs` that has a row."""
    rows = await tx.fetch(
        _query(
            f"SELECT DISTINCT ON (topic, identity) topic, identity, meta FROM {{}} WHERE {PAIRS} "
            "ORDER BY topic, identity, id DESC"
        ),
        _columns(pairs),
    )
    return {(topic, identity): meta for topic, identity, meta in rows}


def _tracked(topic: str) -> bool:
    # A longer topic cannot be stored, and broadcast() refuses it anyway.
    return len(topic) <= TOPIC_MAX_LENGTH and topic.startswith(tuple(conf.get("PRESENCE_TOPICS")))


def _columns(pairs: list[Pair]) -> list[list[str]]:
    return [[topic for topic, _ in pairs], [identity for _, identity in pairs]]


async def _lock(tx: Transaction, pairs: list[Pair]) -> None:
    # One statement, in the order of the lock keys: string order may differ when two hashes collide.
    await tx.fetch(
        sql.SQL(
            "SELECT pg_advisory_xact_lock(a, b) FROM (SELECT DISTINCT hashtext(topic) AS a, hashtext(identity) AS b "
            "FROM unnest(%s::text[], %s::text[]) AS pair(topic, identity) ORDER BY a, b) AS keys"
        ),
        _columns(pairs),
    )


async def _report(changes: Changes, kind: Literal["joined", "left", "updated"]) -> None:
    by_topic: dict[str, list[Presence]] = {}
    for (topic, identity), meta in changes.items():
        by_topic.setdefault(topic, []).append(Presence(identity, meta))
    for topic, entries in sorted(by_topic.items()):
        named: dict[str, tuple[Presence, ...]] = {"joined": (), "left": (), "updated": ()}
        named[kind] = tuple(entries)
        await signals.asend(presence_changed, Presence, topic=topic, **named)


def _query(text: str) -> sql.Composed:
    # text is a literal from this module, never user input.
    return sql.SQL(text).format(sql.Identifier(PresenceEntry._meta.db_table))  # pyright: ignore[reportArgumentType]


_CLOSE = _query("UPDATE {} SET closed_at = now() WHERE stream = ANY(%s) AND closed_at IS NULL RETURNING id")


def present(topic: str) -> list[Presence]:
    """The readers of `topic`, each identity once, in the order they joined, with their newest meta.

    Expired rows and rows closed past the grace period do not count, even before a sweep deletes them.
    """
    gone = RawSQL(GONE, [GRACE_SECONDS, EXPIRE_SECONDS], output_field=BooleanField())
    # The hub's alias: a router could send the read to a standby, which refuses UNLOGGED tables.
    entries = PresenceEntry.objects.using(hub_database().alias).filter(topic=topic).exclude(gone).order_by("id")
    readers: dict[str, Presence] = {}
    for identity, meta in entries.values_list("identity", "meta"):
        # The position of the first row, the meta of the newest, such as a new name.
        readers[identity] = Presence(identity, meta)
    return list(readers.values())


async def apresent(topic: str) -> list[Presence]:
    """present() for async code."""
    return await sync_to_async(present)(topic)


async def stream_opened_receiver(sender: Any, stream: StreamInfo, **kwargs: Any) -> None:  # noqa: ARG001
    """Connected to stream_opened by the app config."""
    await get_tracker().join(stream)


async def stream_closed_receiver(sender: Any, stream: StreamInfo, **kwargs: Any) -> None:  # noqa: ARG001
    """Connected to stream_closed by the app config."""
    # create=False: the stream may close after its hub stopped, and a new hub would have nothing to close.
    if (hub := get_hub(create=False)) is not None and (tracker := hub.extensions.get(_EXTENSION)) is not None:
        await tracker.leave(stream)


_EXTENSION = "django_pg_sse.presence"


def get_tracker() -> Tracker:
    """The tracker of the running event loop's hub; it lives as long as the hub."""
    hub = get_hub()
    if _EXTENSION not in hub.extensions:
        hub.extensions[_EXTENSION] = Tracker(hub)
    return hub.extensions[_EXTENSION]
