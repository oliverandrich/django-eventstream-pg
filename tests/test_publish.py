import threading
from typing import Any
from unittest import mock

import psycopg
from django.contrib.sessions.backends.db import SessionStore
from django.db import connection, transaction
from django.db.backends.utils import CursorWrapper
from django.test import SimpleTestCase, TestCase, TransactionTestCase
from psycopg import sql

from django_eventstream_pg import hub as hub_module
from django_eventstream_pg import messages, publish
from django_eventstream_pg.models import StreamEvent


def listener() -> psycopg.Connection[tuple[Any, ...]]:
    """A connection of its own that receives the NOTIFY broadcast() sends."""
    conn = psycopg.connect(**hub_module.connect_params(), autocommit=True)
    conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(messages.CHANNEL)))
    return conn


def notified_ids(conn: psycopg.Connection[tuple[Any, ...]], count: int) -> list[int]:
    notifies = conn.notifies(timeout=5, stop_after=count)
    received = [parsed[0] for n in notifies if (parsed := messages.parse_payload(n.payload))]
    return received


class FormatTest(SimpleTestCase):
    def test_single_line(self):
        event = messages.Message(7, "public:news", "", "<p>hi</p>")
        self.assertEqual(messages.format_event(event, "7"), "id: 7\ndata: <p>hi</p>\n\n")

    def test_each_line_gets_a_data_field(self):
        event = messages.Message(8, "public:news", "", "<ul>\n<li>a</li>\n</ul>")
        self.assertEqual(messages.format_event(event, "8"), "id: 8\ndata: <ul>\ndata: <li>a</li>\ndata: </ul>\n\n")

    def test_named_event(self):
        event = messages.Message(9, "user:1", "progress", "50")
        self.assertEqual(messages.format_event(event, "9"), "id: 9\nevent: progress\ndata: 50\n\n")

    def test_a_carriage_return_cannot_start_a_field(self):
        # SSE ends a line at CR, LF or CRLF; user text with a lone CR must stay data.
        event = messages.Message(10, "public:news", "", "a\rid: 999\r\nb")
        expected = "id: 10\ndata: a\ndata: id: 999\ndata: b\n\n"
        self.assertEqual(messages.format_event(event, "10"), expected)

    def test_event_names_reject_line_breaks(self):
        with self.assertRaises(ValueError):
            publish.broadcast("public:news", "x", event="a\ndata: b")

    def test_payload_carries_id_and_topic(self):
        self.assertEqual(messages.parse_payload("12:room:3"), (12, "room:3"))
        self.assertIsNone(messages.parse_payload("ping"))

    def test_a_signed_id_resumes_only_its_own_scope(self):
        owner = messages.Owner.of(user="7", session="abc")
        topics = ["public:news", "user:7"]
        signed = messages.EventIds(owner, topics).sign(42)
        self.assertEqual(messages.EventIds(owner, topics).unsign(signed), 42)
        # The topic order of the query string does not matter.
        self.assertEqual(messages.EventIds(owner, ["user:7", "public:news"]).unsign(signed), 42)
        for other_owner, other_topics in [
            (messages.Owner.of(user="8", session="abc"), topics),
            (messages.Owner.of(user="7", session="new"), topics),
            (messages.Owner(frozenset()), topics),
            (owner, ["public:news"]),
        ]:
            self.assertIsNone(messages.EventIds(other_owner, other_topics).unsign(signed))

    def test_an_unsigned_or_forged_id_resumes_nothing(self):
        ids = messages.EventIds(messages.Owner(frozenset()), ["public:news"])
        for value in ("", "42", "42:forged", ids.sign(42) + "0"):
            self.assertIsNone(ids.unsign(value), value)

    def test_last_event_id_must_be_a_plain_number(self):
        self.assertEqual(messages.parse_last_event_id("42"), 42)
        for value in ("", "abc", "²", "-1"):
            self.assertIsNone(messages.parse_last_event_id(value), value)


class CloseEventTest(TestCase):
    def test_a_close_event_holds_no_session_key(self):
        # Close events are stored and replayed; a raw session key would allow a session takeover.
        session = SessionStore()
        session.create()
        assert session.session_key is not None
        with self.captureOnCommitCallbacks(execute=True):
            publish.close_streams(session=session)
        self.assertNotIn(session.session_key, StreamEvent.objects.get().html)


class BroadcastTest(TestCase):
    def test_writes_the_event_when_the_transaction_commits(self):
        with self.captureOnCommitCallbacks(execute=True) as callbacks:
            publish.broadcast("public:news", "<p>hi</p>")
            self.assertFalse(StreamEvent.objects.exists())
        self.assertEqual(len(callbacks), 1)
        event = StreamEvent.objects.get()
        self.assertEqual((event.topic, event.html, event.event), ("public:news", "<p>hi</p>", ""))

    def test_a_failed_publish_is_logged_not_raised(self):
        # The caller's transaction has committed; a 500 would invite a duplicate retry.
        failure = mock.patch.object(StreamEvent.objects, "create", side_effect=RuntimeError("down"))
        # Production logs to django.db.backends.base; captureOnCommitCallbacks to django.test.
        with failure, self.assertLogs("django", "ERROR"):
            with self.captureOnCommitCallbacks(execute=True):
                publish.broadcast("public:news", "<p>hi</p>")

    def test_a_rollback_sends_nothing(self):
        with self.captureOnCommitCallbacks(execute=True):
            with self.assertRaises(RuntimeError), transaction.atomic():
                publish.broadcast("public:news", "<p>hi</p>")
                raise RuntimeError
        self.assertFalse(StreamEvent.objects.exists())


class NotifyTest(TransactionTestCase):
    def test_notifies_listeners_with_the_event_id(self):
        conn = self.enterContext(listener())
        publish.broadcast("public:news", "<p>hi</p>")
        self.assertEqual(notified_ids(conn, 1), [StreamEvent.objects.get().id])

    def test_events_become_visible_in_id_order(self):
        # Replay after Last-Event-ID is exact only if no smaller id commits after a larger one.
        conn = self.enterContext(listener())
        first_holds_the_lock = threading.Event()
        original_notify = publish._notify  # pyright: ignore[reportPrivateUsage]

        def slow_notify(cursor: CursorWrapper, event_id: int, topic: str) -> None:
            if not first_holds_the_lock.is_set():
                first_holds_the_lock.set()
                threading.Event().wait(0.5)
            original_notify(cursor, event_id, topic)

        def send(text: str) -> None:
            publish.broadcast("public:news", text)
            connection.close()

        with mock.patch.object(publish, "_notify", slow_notify):
            first = threading.Thread(target=send, args=("first",))
            first.start()
            first_holds_the_lock.wait(5)
            second = threading.Thread(target=send, args=("second",))
            second.start()
            first.join()
            second.join()

        received = notified_ids(conn, 2)
        self.assertEqual(received, sorted(received))
        ids = dict(StreamEvent.objects.values_list("html", "id"))
        self.assertLess(ids["first"], ids["second"])
