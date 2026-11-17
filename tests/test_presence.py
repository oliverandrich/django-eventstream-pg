import asyncio
import contextlib
from typing import Any
from unittest import mock

from asgiref.sync import sync_to_async
from django.contrib.auth.models import User
from django.db import connection
from django.http import HttpRequest
from django.test import AsyncClient, RequestFactory, SimpleTestCase, override_settings
from psycopg import sql

from django_pg_sse import hub as hub_module
from django_pg_sse.messages import digest
from django_pg_sse.presence import Presence, apresent, presence_changed, present, tracker
from django_pg_sse.presence import checks as presence_checks
from django_pg_sse.signals import StreamInfo
from tests.test_stream import Stream, StreamTestCase, stops_the_hub

ROOM = "room:1"
PRESENCE = {
    "CAN_SUBSCRIBE": "tests.test_stream.allow_every_topic",
    "PRESENCE_TOPICS": ["room:"],
}


def name_from_session(request: HttpRequest) -> tuple[str, dict[str, Any]] | None:
    """An identity for visitors without an account, who picked a name."""
    if "name" not in request.session:
        return None
    session_key = request.session.session_key
    assert session_key is not None
    # A digest: every reader of the topic sees the identity, and a session key would let them take over the session.
    return f"session:{digest(session_key)}", {"name": request.session["name"]}


@override_settings(PG_SSE=PRESENCE)
class PresenceTestCase(StreamTestCase):
    def setUp(self) -> None:
        self.changes: list[tuple[str, tuple[Presence, ...], tuple[Presence, ...]]] = []
        self.updates: list[tuple[str, tuple[Presence, ...]]] = []
        self.streams: list[Stream] = []

        def receiver(sender: Any, topic: str, joined: Any, left: Any, **kwargs: Any) -> None:  # noqa: ARG001
            if updated := kwargs.get("updated"):
                self.updates.append((topic, updated))
            else:
                self.changes.append((topic, joined, left))

        presence_changed.connect(receiver, weak=False)
        self.addCleanup(presence_changed.disconnect, receiver)
        # Short enough for a test, long enough for a reconnect inside it.
        self.enterContext(mock.patch.object(tracker, "GRACE_SECONDS", 0.3))

    async def open_stream(
        self, *topics: str, client: AsyncClient | None = None, headers: dict[str, str] | None = None
    ) -> Stream:
        stream = await super().open_stream(*topics, client=client, headers=headers)
        # A dropped stream is finalised whenever the garbage collector runs, which closes it and leaves.
        self.streams.append(stream)
        return stream

    async def client_for(self, name: str) -> tuple[AsyncClient, User]:
        user = await User.objects.acreate_user(name)
        client = AsyncClient()
        await client.aforce_login(user)
        return client, user

    async def close(self, stream: Stream) -> None:
        # Django cancels the response task when the client disconnects.
        waiting = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        waiting.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await waiting

    async def settle(self, seconds: float = 0.6) -> None:
        """Wait past the grace period, so a pending sweep has run."""
        await asyncio.sleep(seconds)

    def info_for(self, user: User, n: int) -> StreamInfo:
        """A stream's signal info without opening the stream."""
        request = RequestFactory().get("/events/")
        request.user = user
        return StreamInfo(id=f"{n:032x}", user_pk=str(user.pk), session=None, topics=(ROOM,), request=request)

    def insert_row(self, identity: str, *, closed: bool = False) -> None:
        """A row an hour past its heartbeat or its close, as a killed worker or a finished grace period leaves."""
        last_seen, closed_at = (
            ("now()", "now() - interval '1 hour'") if closed else ("now() - interval '1 hour'", "NULL")
        )
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO django_pg_sse_presence (stream, topic, identity, meta, last_seen, closed_at)"
                f" VALUES ('gone', %s, %s, '{{}}', {last_seen}, {closed_at})",
                [ROOM, identity],
            )

    def rows(self) -> int:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM django_pg_sse_presence")
            return cursor.fetchone()[0]


class JoinTest(PresenceTestCase):
    @stops_the_hub
    async def test_a_first_stream_joins_the_topic(self):
        client, user = await self.client_for("ada")
        await self.open_stream(ROOM, client=client)
        ada = Presence(f"user:{user.pk}", {"name": "ada"})
        self.assertEqual(self.changes, [(ROOM, (ada,), ())])
        self.assertEqual(await apresent(ROOM), [ada])
        self.assertEqual(await sync_to_async(present)(ROOM), [ada])

    @stops_the_hub
    async def test_a_second_tab_does_not_join_again(self):
        client, _ = await self.client_for("ada")
        await self.open_stream(ROOM, client=client)
        await self.open_stream(ROOM, client=client)
        self.assertEqual(len(self.changes), 1)
        self.assertEqual(len(await apresent(ROOM)), 1)

    @stops_the_hub
    async def test_tabs_opening_at_once_report_one_join(self):
        # Two workers may open streams of one identity at the same moment.
        user = await User.objects.acreate_user("ada")
        infos = [self.info_for(user, n) for n in range(4)]
        # Four open pool connections, so the joins run in parallel rather than one after another.
        pool = hub_module.get_hub().streams
        await asyncio.gather(*(pool.fetch(sql.SQL("SELECT pg_sleep(0.2)"), []) for _ in infos))
        # Four trackers stand in for four workers; Hub.stop() stops each.
        trackers = [tracker.Tracker(hub_module.get_hub()) for _ in infos]
        await asyncio.gather(*(each.join(info) for each, info in zip(trackers, infos, strict=True)))
        self.assertEqual(len(self.changes), 1)

    @stops_the_hub
    async def test_present_lists_each_identity_in_the_order_it_joined(self):
        ada_client, ada = await self.client_for("ada")
        grace_client, grace = await self.client_for("grace")
        await self.open_stream(ROOM, client=ada_client)
        await self.open_stream(ROOM, client=grace_client)
        identities = [entry.identity for entry in await apresent(ROOM)]
        self.assertEqual(identities, [f"user:{ada.pk}", f"user:{grace.pk}"])

    @stops_the_hub
    async def test_topics_outside_presence_topics_write_nothing(self):
        client, _ = await self.client_for("ada")
        await self.open_stream("public:news", client=client)
        self.assertEqual(self.changes, [])
        self.assertEqual(await sync_to_async(self.rows)(), 0)

    @stops_the_hub
    async def test_a_stream_without_an_identity_is_not_tracked(self):
        # The default identity covers logged-in users only.
        await self.open_stream(ROOM)
        self.assertEqual(self.changes, [])
        self.assertEqual(await apresent(ROOM), [])

    @stops_the_hub
    async def test_the_identity_setting_tracks_visitors_without_an_account(self):
        client = AsyncClient()
        session = await client.asession()
        await session.aset("name", "Ada")
        await session.asave()
        with override_settings(PG_SSE={**PRESENCE, "PRESENCE_IDENTITY": "tests.test_presence.name_from_session"}):
            await self.open_stream(ROOM, client=client)
        ((topic, (joined,), ()),) = self.changes
        self.assertEqual((topic, joined.meta), (ROOM, {"name": "Ada"}))
        assert session.session_key is not None
        self.assertEqual(joined.identity, f"session:{digest(session.session_key)}")

    @stops_the_hub
    async def test_a_topic_too_long_to_store_is_not_tracked(self):
        client, _ = await self.client_for("ada")
        with self.assertNoLogs("django.dispatch", "ERROR"):
            await self.open_stream("room:" + "x" * 200, client=client)
        self.assertEqual(self.changes, [])


class LeaveTest(PresenceTestCase):
    @stops_the_hub
    async def test_the_last_tab_leaves_after_the_grace_period(self):
        client, user = await self.client_for("ada")
        stream = await self.open_stream(ROOM, client=client)
        await self.close(stream)
        self.assertEqual(len(self.changes), 1)
        await self.settle()
        ada = Presence(f"user:{user.pk}", {"name": "ada"})
        self.assertEqual(self.changes[1:], [(ROOM, (), (ada,))])
        self.assertEqual(await apresent(ROOM), [])

    @stops_the_hub
    async def test_closing_one_of_two_tabs_does_not_leave(self):
        client, _ = await self.client_for("ada")
        first = await self.open_stream(ROOM, client=client)
        await self.open_stream(ROOM, client=client)
        await self.close(first)
        await self.settle()
        self.assertEqual(len(self.changes), 1)
        self.assertEqual(len(await apresent(ROOM)), 1)

    @stops_the_hub
    async def test_a_reconnect_within_the_grace_period_changes_nothing(self):
        client, _ = await self.client_for("ada")
        stream = await self.open_stream(ROOM, client=client)
        await self.close(stream)
        await self.open_stream(ROOM, client=client)
        await self.settle()
        self.assertEqual(len(self.changes), 1)
        self.assertEqual(len(await apresent(ROOM)), 1)

    @stops_the_hub
    async def test_a_join_cancelled_after_its_commit_still_leaves(self):
        # The client may disconnect while stream_opened receivers run.
        user = await User.objects.acreate_user("ada")
        info = self.info_for(user, 1)
        own = tracker.Tracker(hub_module.get_hub())
        pool = own.hub.streams
        connect = pool.connection

        @contextlib.asynccontextmanager
        async def cancelled_after_commit():
            async with connect() as conn:
                yield conn
            raise asyncio.CancelledError

        with (
            mock.patch.object(pool, "connection", cancelled_after_commit),
            self.assertRaises(asyncio.CancelledError),
        ):
            await own.join(info)
        await own.leave(info)
        await self.settle()
        self.assertEqual(await apresent(ROOM), [])
        self.assertEqual(await sync_to_async(self.rows)(), 0)

    @stops_the_hub
    async def test_many_closes_share_one_sweep(self):
        # A deploy or a proxy restart closes every stream at once.
        users = [await User.objects.acreate_user(f"user{n}") for n in range(10)]
        infos = [self.info_for(user, n) for n, user in enumerate(users)]
        own = tracker.get_tracker()
        for info in infos:
            await own.join(info)
        sweep = own.sweep
        sweeps: list[None] = []

        async def counted(**kwargs: Any) -> None:
            sweeps.append(None)
            await sweep(**kwargs)

        with mock.patch.object(own, "sweep", counted):
            for n, info in enumerate(infos):
                await own.leave(info)
                if n == len(infos) // 2:
                    # The later closes fall into the pending sweep's grace period.
                    await asyncio.sleep(tracker.GRACE_SECONDS / 2)
            await self.settle(1.0)
        self.assertLessEqual(len(sweeps), 2)
        # Each close gets its leave, also those after the pending sweep was scheduled.
        left = [entry for _, _, gone in self.changes for entry in gone]
        self.assertEqual(len(left), len(users))

    @stops_the_hub
    async def test_a_stopped_tracker_ignores_a_late_join(self):
        # Hub.stop() may run while a join waits for the identity function.
        user = await User.objects.acreate_user("ada")
        own = tracker.get_tracker()
        await own.stop()
        await own.join(self.info_for(user, 1))
        self.assertEqual((own.streams, own.heartbeat), ({}, None))
        self.assertEqual(await sync_to_async(self.rows)(), 0)

    @stops_the_hub
    async def test_stopping_closes_the_rows_of_its_streams(self):
        # Other workers then report the leaves after the grace period, not after the expiry.
        user = await User.objects.acreate_user("ada")
        await tracker.get_tracker().join(self.info_for(user, 1))
        await hub_module.get_hub().stop()

        def open_rows() -> int:
            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM django_pg_sse_presence WHERE closed_at IS NULL")
                return cursor.fetchone()[0]

        self.assertEqual(await sync_to_async(open_rows)(), 0)

    @stops_the_hub
    async def test_a_stopped_hub_is_not_started_again(self):
        user = await User.objects.acreate_user("ada")
        info = self.info_for(user, 1)
        await tracker.get_tracker().join(info)
        await hub_module.get_hub().stop()
        # The stream closes after its hub stopped, as at loop shutdown.
        await tracker.stream_closed_receiver(sender=StreamInfo, stream=info)
        self.assertNotIn(asyncio.get_running_loop(), hub_module._hubs)  # pyright: ignore[reportPrivateUsage]


class UpdateTest(PresenceTestCase):
    async def rename(self, user: User, name: str) -> None:
        user.username = name
        await user.asave()

    @stops_the_hub
    async def test_present_shows_the_newest_meta_in_the_first_place(self):
        ada = await User.objects.acreate_user("ada")
        grace = await User.objects.acreate_user("grace")
        own = tracker.get_tracker()
        await own.join(self.info_for(ada, 1))
        await own.join(self.info_for(grace, 2))
        await self.rename(ada, "ada lovelace")
        await own.join(self.info_for(ada, 3))
        self.assertEqual(
            await apresent(ROOM),
            [Presence(f"user:{ada.pk}", {"name": "ada lovelace"}), Presence(f"user:{grace.pk}", {"name": "grace"})],
        )

    @stops_the_hub
    async def test_a_changed_meta_is_reported_as_updated(self):
        # A rename reconnects within the grace period, so no leave and join tell the other readers.
        ada = await User.objects.acreate_user("ada")
        own = tracker.get_tracker()
        await own.join(self.info_for(ada, 1))
        await self.rename(ada, "ada lovelace")
        await own.join(self.info_for(ada, 2))
        self.assertEqual(len(self.changes), 1)
        self.assertEqual(self.updates, [(ROOM, (Presence(f"user:{ada.pk}", {"name": "ada lovelace"}),))])

    @stops_the_hub
    async def test_an_unchanged_meta_reports_nothing(self):
        ada = await User.objects.acreate_user("ada")
        own = tracker.get_tracker()
        await own.join(self.info_for(ada, 1))
        await own.join(self.info_for(ada, 2))
        self.assertEqual((len(self.changes), self.updates), (1, []))


class ExpiryTest(PresenceTestCase):
    @stops_the_hub
    async def test_the_heartbeat_refills_an_emptied_table(self):
        # A crash empties an UNLOGGED table while the streams stay open.
        self.enterContext(mock.patch.object(tracker, "HEARTBEAT_SECONDS", 0.1))
        client, user = await self.client_for("ada")
        await self.open_stream(ROOM, client=client)
        await sync_to_async(self.truncate)()
        await asyncio.sleep(0.4)
        ada = Presence(f"user:{user.pk}", {"name": "ada"})
        self.assertEqual(await apresent(ROOM), [ada])
        # Other workers may have reported a leave for the lost rows meanwhile.
        self.assertEqual(self.changes, [(ROOM, (ada,), ()), (ROOM, (ada,), ())])

    def truncate(self) -> None:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE django_pg_sse_presence")

    @stops_the_hub
    async def test_rows_of_a_killed_worker_expire(self):
        await sync_to_async(self.insert_row)("user:99")
        await tracker.get_tracker().sweep()
        self.assertEqual(self.changes, [(ROOM, (), (Presence("user:99", {}),))])
        self.assertEqual(await apresent(ROOM), [])

    @stops_the_hub
    async def test_present_skips_rows_past_their_expiry(self):
        # A row stays until some worker sweeps it; present() must not wait for that.
        await sync_to_async(self.insert_row)("user:99")
        self.assertEqual(await apresent(ROOM), [])

    @stops_the_hub
    async def test_present_skips_rows_closed_past_the_grace_period(self):
        await sync_to_async(self.insert_row)("user:99", closed=True)
        self.assertEqual(await apresent(ROOM), [])

    @stops_the_hub
    async def test_concurrent_sweeps_report_a_leave_once(self):
        # Every worker sweeps; only one may report each leave.
        await sync_to_async(self.insert_row)("user:99")
        await asyncio.gather(*(tracker.get_tracker().sweep() for _ in range(4)))
        self.assertEqual(len(self.changes), 1)

    @stops_the_hub
    async def test_the_heartbeat_keeps_open_streams_present(self):
        self.enterContext(mock.patch.object(tracker, "HEARTBEAT_SECONDS", 0.1))
        self.enterContext(mock.patch.object(tracker, "EXPIRE_SECONDS", 1.0))
        client, _ = await self.client_for("ada")
        await self.open_stream(ROOM, client=client)
        # Longer than the expiry, which ten heartbeats each push back.
        await asyncio.sleep(1.6)
        self.assertEqual(len(self.changes), 1, self.changes)
        self.assertEqual(len(await apresent(ROOM)), 1)


class PresenceCheckTest(SimpleTestCase):
    def test_presence_topics_must_be_a_list_of_strings(self):
        for value in ("room:", ["room:", 1], None):
            with override_settings(PG_SSE={"PRESENCE_TOPICS": value}):
                errors = presence_checks.check_presence_topics(None)
            self.assertEqual([error.id for error in errors], ["pg_sse.E006"], value)
        self.assertEqual(presence_checks.check_presence_topics(None), [])

    def test_presence_identity_must_name_a_function(self):
        for value in ("tests.test_presence.missing", 7):
            with override_settings(PG_SSE={"PRESENCE_IDENTITY": value}):
                errors = presence_checks.check_presence_identity(None)
            self.assertEqual([error.id for error in errors], ["pg_sse.E007"], value)
        self.assertEqual(presence_checks.check_presence_identity(None), [])
