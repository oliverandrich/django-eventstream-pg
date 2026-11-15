import asyncio
import functools
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import Any
from unittest import mock

from asgiref.sync import sync_to_async
from django.contrib.auth.base_user import AbstractBaseUser
from django.contrib.auth.models import AnonymousUser, User
from django.contrib.sessions.backends.db import SessionStore
from django.db import DEFAULT_DB_ALIAS, connection, connections, transaction
from django.http import HttpRequest, HttpResponseBase, StreamingHttpResponse
from django.test import AsyncClient, RequestFactory, SimpleTestCase, TransactionTestCase, override_settings
from psycopg import sql

from django_eventstream_pg import hub as hub_module
from django_eventstream_pg import messages, permissions, publish, views
from django_eventstream_pg.models import StreamEvent

type Stream = AsyncIterator[bytes]


class CanSubscribeTest(SimpleTestCase):
    def check(self, user: AbstractBaseUser | AnonymousUser, topic: str) -> bool:
        request = RequestFactory().get("/events/")
        request.user = user
        return permissions.can_subscribe(request, topic)

    def test_anyone_may_read_public_topics(self):
        self.assertTrue(self.check(AnonymousUser(), "public:news"))

    def test_a_user_topic_belongs_to_that_user_alone(self):
        user = User(pk=7)
        self.assertTrue(self.check(user, "user:7"))
        self.assertFalse(self.check(user, "user:8"))
        self.assertFalse(self.check(AnonymousUser(), "user:7"))

    def test_other_topics_are_closed_until_the_project_opens_them(self):
        self.assertFalse(self.check(User(pk=7), "room:1"))


def allow_every_topic(request: HttpRequest, topic: str) -> bool:  # noqa: ARG001
    return True


def terminate_listener() -> None:
    """Ends every hub's LISTEN session, as a database restart would."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE %s", ["LISTEN %"])


def notify(payload: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_notify(%s, %s)", [messages.CHANNEL, payload])


def stops_the_hub[T: TransactionTestCase](test: Callable[[T], Awaitable[None]]) -> Callable[[T], Awaitable[None]]:
    """Close the hub's connections in the test's own event loop.

    Django's test cases run no asyncTearDown, and the loop ends with the test.
    """

    @functools.wraps(test)
    async def wrapper(self: T) -> None:
        try:
            await test(self)
        finally:
            await hub_module.get_hub().stop()

    return wrapper


def content_of(response: HttpResponseBase) -> Stream:
    assert isinstance(response, StreamingHttpResponse)
    content = response.streaming_content
    # An async view streams an async iterator, which an ASGI server sends without buffering.
    assert isinstance(content, AsyncIterable)
    return aiter(content)


class StreamTestCase(TransactionTestCase):
    async def open_stream(
        self, *topics: str, client: AsyncClient | None = None, headers: dict[str, str] | None = None
    ) -> Stream:
        """A stream whose subscription is in place, after its opening comment."""
        query = "&".join(f"topic={topic}" for topic in topics)
        response = await (client or AsyncClient()).get(f"/events/?{query}", headers=headers or {})
        self.assertEqual(response["Content-Type"], "text/event-stream")
        stream = content_of(response)
        self.assertEqual(await asyncio.wait_for(anext(stream), 5), messages.CONNECTED.encode())
        return stream

    async def next_message(self, stream: Stream, timeout: float = 5) -> str:
        """The next message, skipping comments such as keepalives."""
        while True:
            chunk = (await asyncio.wait_for(anext(stream), timeout)).decode()
            if not chunk.startswith(":"):
                return chunk


class StreamTest(StreamTestCase):
    async def test_requires_a_topic(self):
        response = await AsyncClient().get("/events/")
        self.assertEqual(response.status_code, 400)

    async def test_refuses_a_topic_the_user_may_not_read(self):
        response = await AsyncClient().get("/events/?topic=public:news&topic=room:1")
        self.assertEqual(response.status_code, 403)

    async def test_works_with_atomic_requests(self):
        # Django refuses an async view inside ATOMIC_REQUESTS unless the view opts out.
        with mock.patch.dict(connections.settings[DEFAULT_DB_ALIAS], ATOMIC_REQUESTS=True):
            response = await AsyncClient().get("/events/")
        self.assertEqual(response.status_code, 400)

    @override_settings(EVENTSTREAM_PG={"CAN_SUBSCRIBE": "tests.test_stream.allow_every_topic"})
    async def test_refuses_the_close_topic_whatever_can_subscribe_allows(self):
        response = await AsyncClient().get(f"/events/?topic={messages.CLOSE_TOPIC}")
        self.assertEqual(response.status_code, 403)

    @stops_the_hub
    async def test_returns_its_connection_after_the_response_middleware(self):
        # Middleware such as SESSION_SAVE_EVERY_REQUEST queries after the view returns.
        with mock.patch.object(views, "connection") as view_connection:
            response = await AsyncClient().get("/events/?topic=public:news")
            view_connection.close.assert_not_called()
            await asyncio.wait_for(anext(content_of(response)), 5)
        view_connection.close.assert_called_once_with()

    @stops_the_hub
    async def test_an_unexpected_error_restarts_the_listener(self):
        stream = await self.open_stream("public:news")
        hub = hub_module.get_hub()
        receive = hub._receive  # pyright: ignore[reportPrivateUsage]
        failed: list[str] = []

        async def fail_once(payload: str) -> None:
            if not failed:
                failed.append(payload)
                raise RuntimeError(payload)
            await receive(payload)

        with mock.patch.object(hub, "_receive", fail_once), self.assertLogs(hub_module.logger, "ERROR"):
            await sync_to_async(publish.broadcast)("public:news", "<p>first</p>")
            self.assertIn("<p>first</p>", await self.next_message(stream, timeout=10))

    @stops_the_hub
    async def test_delivers_a_broadcast_to_the_open_stream(self):
        stream = await self.open_stream("public:news")
        await sync_to_async(publish.broadcast)("public:news", "<p>hi</p>")
        message = await self.next_message(stream)
        event = await StreamEvent.objects.aget()
        self.assertEqual(message, f"id: {event.id}\ndata: <p>hi</p>\n\n")

    @stops_the_hub
    async def test_other_topics_do_not_reach_the_stream(self):
        stream = await self.open_stream("public:news")
        await sync_to_async(publish.broadcast)("public:other", "<p>no</p>")
        await sync_to_async(publish.broadcast)("public:news", "<p>yes</p>")
        self.assertIn("<p>yes</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_last_event_id_replays_missed_events(self):
        seen = await StreamEvent.objects.acreate(topic="public:news", html="seen")
        missed = await StreamEvent.objects.acreate(topic="public:news", html="missed")
        await StreamEvent.objects.acreate(topic="public:other", html="elsewhere")
        stream = await self.open_stream("public:news", headers={"Last-Event-ID": str(seen.id)})
        self.assertEqual(await self.next_message(stream), f"id: {missed.id}\ndata: missed\n\n")

    @stops_the_hub
    async def test_a_foreign_notify_leaves_the_listener_running(self):
        stream = await self.open_stream("public:news")
        with self.assertLogs(hub_module.logger, "WARNING"):
            await sync_to_async(notify)("ping")
            await sync_to_async(publish.broadcast)("public:news", "<p>still</p>")
            self.assertIn("<p>still</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_a_long_replay_ends_the_stream_after_one_batch(self):
        # htmx reconnects with the last id it got, so the next batch follows.
        first = await StreamEvent.objects.acreate(topic="public:news", html="one")
        await StreamEvent.objects.acreate(topic="public:news", html="two")
        await StreamEvent.objects.acreate(topic="public:news", html="three")
        with mock.patch.object(hub_module, "REPLAY_BATCH", 1):
            stream = await self.open_stream("public:news", headers={"Last-Event-ID": str(first.id)})
            self.assertIn("two", await self.next_message(stream))
            with self.assertRaises(StopAsyncIteration):
                await self.next_message(stream)

    @stops_the_hub
    async def test_an_unread_stream_leaves_no_subscription(self):
        request = RequestFactory().get("/events/?topic=public:news")
        request.user = AnonymousUser()
        request.session = SessionStore()
        response = await views.stream(request)
        assert isinstance(response, StreamingHttpResponse)
        content: Any = response.streaming_content
        await content.aclose()
        self.assertEqual(hub_module.get_hub().queues, {})

    @stops_the_hub
    async def test_events_sent_while_the_listener_reconnects_arrive(self):
        stream = await self.open_stream("public:news")
        with self.assertLogs(hub_module.logger, "WARNING"):
            await sync_to_async(terminate_listener)()
            await sync_to_async(publish.broadcast)("public:news", "<p>during</p>")
            self.assertIn("<p>during</p>", await self.next_message(stream, timeout=10))


class CloseStreamsTest(StreamTestCase):
    async def logged_in_client(self, user: User) -> AsyncClient:
        client = AsyncClient()
        await client.aforce_login(user)
        return client

    async def assert_closed(self, stream: Stream) -> None:
        self.assertEqual(await self.next_message(stream), messages.CLOSED)
        with self.assertRaises(StopAsyncIteration):
            await self.next_message(stream)

    @stops_the_hub
    async def test_logout_closes_the_streams_of_that_session_only(self):
        user = await User.objects.acreate_user("ada")
        first, second = await self.logged_in_client(user), await self.logged_in_client(user)
        first_stream = await self.open_stream(f"user:{user.pk}", client=first)
        second_stream = await self.open_stream(f"user:{user.pk}", client=second)
        await first.alogout()
        await self.assert_closed(first_stream)
        await sync_to_async(publish.broadcast)(f"user:{user.pk}", "<p>still here</p>")
        self.assertIn("still here", await self.next_message(second_stream))

    @stops_the_hub
    async def test_closing_by_user_closes_every_session(self):
        user = await User.objects.acreate_user("ada")
        streams = [
            await self.open_stream(f"user:{user.pk}", client=await self.logged_in_client(user)) for _ in range(2)
        ]
        await sync_to_async(publish.close_streams)(user=user)
        for stream in streams:
            await self.assert_closed(stream)

    @stops_the_hub
    async def test_a_rolled_back_close_leaves_the_stream_open(self):
        user = await User.objects.acreate_user("ada")
        stream = await self.open_stream(f"user:{user.pk}", client=await self.logged_in_client(user))

        def close_then_roll_back() -> None:
            with transaction.atomic():
                publish.close_streams(user=user)
                transaction.set_rollback(True)

        await sync_to_async(close_then_roll_back)()
        await sync_to_async(publish.broadcast)(f"user:{user.pk}", "<p>open</p>")
        self.assertIn("open", await self.next_message(stream))

    @stops_the_hub
    async def test_a_close_sent_while_the_listener_reconnects_arrives(self):
        user = await User.objects.acreate_user("ada")
        stream = await self.open_stream(f"user:{user.pk}", client=await self.logged_in_client(user))
        with self.assertLogs(hub_module.logger, "WARNING"):
            await sync_to_async(terminate_listener)()
            await sync_to_async(publish.close_streams)(user=user)
            await self.assert_closed(stream)

    @stops_the_hub
    async def test_a_close_before_the_stream_starts_reaches_it(self):
        # The view authorises; the stream subscribes later, when its first chunk is read.
        user = await User.objects.acreate_user("ada")
        client = await self.logged_in_client(user)
        stream = content_of(await client.get(f"/events/?topic=user:{user.pk}"))
        await sync_to_async(publish.close_streams)(user=user)
        self.assertEqual(await asyncio.wait_for(anext(stream), 5), messages.CONNECTED.encode())
        await self.assert_closed(stream)

    async def test_a_closed_stream_stays_closed_when_its_queue_overflows(self):
        hub = hub_module.Hub()
        owner = messages.Owner.of(user="7", session=None)
        with mock.patch.object(hub_module, "QUEUE_SIZE", 2):
            queue = hub.subscribe(["public:news"], owner)
        try:
            hub._close("user:7")  # pyright: ignore[reportPrivateUsage]
            for event_id in range(1, 5):
                hub._deliver(messages.Message(event_id, "public:news", "", "<p>x</p>"))  # pyright: ignore[reportPrivateUsage]
            self.assertEqual([queue.get_nowait() for _ in range(queue.qsize())], [messages.Signal.REVOKED])
        finally:
            await hub.stop()

    async def test_a_process_without_targets_skips_loading_closes(self):
        hub = hub_module.Hub()
        with mock.patch.object(hub, "_fetch") as fetch:
            await hub._receive(f"5:{messages.CLOSE_TOPIC}")  # pyright: ignore[reportPrivateUsage]
        fetch.assert_not_called()
        self.assertEqual(hub.last_id, 5)


class CloseStreamsArgumentsTest(SimpleTestCase):
    def test_needs_a_user_or_a_session(self):
        with self.assertRaises(ValueError):
            publish.close_streams()

    def test_a_session_without_a_key_has_no_streams(self):
        publish.close_streams(session=SessionStore())


ROLE = "eventstream_pg_test_role"


class AssumeRoleTest(TransactionTestCase):
    def run_sql(self, statement: str) -> None:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL(statement).format(sql.Identifier(ROLE)))  # pyright: ignore[reportArgumentType]

    async def test_hub_connections_take_the_configured_role(self):
        await sync_to_async(self.run_sql)("DROP ROLE IF EXISTS {}")
        await sync_to_async(self.run_sql)("CREATE ROLE {} NOLOGIN")
        self.addCleanup(self.run_sql, "DROP ROLE IF EXISTS {}")
        options = {**connection.settings_dict["OPTIONS"], "assume_role": ROLE}
        hub = hub_module.Hub()
        try:
            with mock.patch.dict(connection.settings_dict, OPTIONS=options):
                rows = await hub._fetch(sql.SQL("SELECT current_user"), [])  # pyright: ignore[reportPrivateUsage]
        finally:
            await hub.stop()
        self.assertEqual(rows, [(ROLE,)])
