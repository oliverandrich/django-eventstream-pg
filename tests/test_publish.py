import threading
from typing import Any

import psycopg
from django.contrib.sessions.backends.db import SessionStore
from django.db import connection, transaction
from django.db.models import CharField
from django.test import SimpleTestCase, TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext
from psycopg import sql

from django_pg_sse import hub as hub_module
from django_pg_sse import messages, publish
from django_pg_sse.models import StreamEvent


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

    def test_reserved_topics_reject_a_broadcast(self):
        with self.assertRaises(ValueError):
            publish.broadcast("_close:user:7", "")

    def test_close_topics_fit_the_topic_column(self):
        owner = messages.Owner.of(user="x" * 300, session="s" * 300)
        field = StreamEvent._meta.get_field("topic")
        assert isinstance(field, CharField)
        limit = field.max_length
        assert limit is not None
        for topic in owner.close_topics:
            self.assertLessEqual(len(topic), limit)

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
            (messages.Owner(()), topics),
            (owner, ["public:news"]),
        ]:
            self.assertIsNone(messages.EventIds(other_owner, other_topics).unsign(signed))

    def test_an_unsigned_or_forged_id_resumes_nothing(self):
        ids = messages.EventIds(messages.Owner(()), ["public:news"])
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
        close = StreamEvent.objects.get()
        self.assertNotIn(session.session_key, close.topic + close.data)


class BroadcastTest(TestCase):
    def test_writes_the_event_when_the_transaction_commits(self):
        with self.captureOnCommitCallbacks(execute=True) as callbacks:
            publish.broadcast("public:news", "<p>hi</p>")
            self.assertFalse(StreamEvent.objects.exists())
        self.assertEqual(len(callbacks), 1)
        event = StreamEvent.objects.get()
        self.assertEqual((event.topic, event.data, event.event), ("public:news", "<p>hi</p>", ""))

    def test_a_failed_publish_is_logged_not_raised(self):
        # The caller's transaction has committed; a 500 would invite a duplicate retry.
        # Production logs to django.db.backends.base; captureOnCommitCallbacks to django.test.
        with self.assertLogs("django", "ERROR"), self.captureOnCommitCallbacks(execute=True):
            publish.broadcast("public:" + "x" * 300, "<p>hi</p>")
        # A savepoint keeps the failure from aborting the surrounding transaction.
        self.assertFalse(StreamEvent.objects.exists())

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
        (notify,) = conn.notifies(timeout=5, stop_after=1)
        # The statement builds the payload in SQL; the hub parses the format of notify_payload().
        self.assertEqual(notify.payload, messages.notify_payload(StreamEvent.objects.get().id, "public:news"))

    def test_publishes_with_one_statement(self):
        # on_commit callbacks run in autocommit, where no savepoint is needed.
        with CaptureQueriesContext(connection) as queries:
            publish.broadcast("public:news", "<p>hi</p>")
        self.assertEqual(len(queries), 1, [query["sql"] for query in queries])

    def test_an_event_takes_its_id_under_the_lock(self):
        # Replay after Last-Event-ID is exact only if no smaller id commits after a larger one.
        holder = self.enterContext(psycopg.connect(**hub_module.connect_params(), autocommit=True))
        holder.execute("SELECT pg_advisory_lock(%s)", [publish.PUBLISH_LOCK_KEY])

        def send() -> None:
            publish.broadcast("public:news", "published")
            connection.close()

        sender = threading.Thread(target=send)
        sender.start()
        self.wait_for_a_lock_waiter(holder)
        table = sql.Identifier(StreamEvent._meta.db_table)
        insert = sql.SQL("INSERT INTO {} (topic, event, data, created) VALUES ('public:news', '', 'direct', now())")
        holder.execute(insert.format(table))
        holder.execute("SELECT pg_advisory_unlock(%s)", [publish.PUBLISH_LOCK_KEY])
        sender.join(5)
        self.assertFalse(sender.is_alive())
        ids = dict(StreamEvent.objects.values_list("data", "id"))
        self.assertLess(ids["direct"], ids["published"])

    def wait_for_a_lock_waiter(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        # An advisory lock on a bigint key splits it into classid (high half) and objid (low half).
        key = publish.PUBLISH_LOCK_KEY & 0xFFFFFFFFFFFFFFFF
        query = """
            SELECT 1 FROM pg_locks JOIN pg_database ON pg_database.oid = pg_locks.database
            WHERE locktype = 'advisory' AND NOT granted AND datname = current_database()
            AND classid::bigint = %s AND objid::bigint = %s
        """
        for _ in range(100):
            if conn.execute(query, [key >> 32, key & 0xFFFFFFFF]).fetchone():
                return
            threading.Event().wait(0.05)
        self.fail("broadcast() did not wait for the advisory lock.")
