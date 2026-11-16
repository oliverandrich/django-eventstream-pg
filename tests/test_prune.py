from datetime import timedelta
from io import StringIO

from django.core.management import call_command
from django.test import TestCase, override_settings
from django.utils import timezone

from django_pg_sse.models import StreamEvent


def make_event(*, age: timedelta) -> StreamEvent:
    event = StreamEvent.objects.create(topic="public:news", html="<p>x</p>")
    StreamEvent.objects.filter(pk=event.pk).update(created=timezone.now() - age)
    return event


class PruneStreamEventsTest(TestCase):
    @override_settings(PG_SSE={"RETENTION_HOURS": 24})
    def test_deletes_events_older_than_the_retention(self):
        old = make_event(age=timedelta(hours=25))
        recent = make_event(age=timedelta(hours=1))
        call_command("prune_stream_events", stdout=StringIO())
        remaining = set(StreamEvent.objects.values_list("id", flat=True))
        self.assertEqual(remaining, {recent.id})
        self.assertNotIn(old.id, remaining)

    def test_hours_option_overrides_the_setting(self):
        make_event(age=timedelta(hours=3))
        call_command("prune_stream_events", "--hours", "2", stdout=StringIO())
        self.assertFalse(StreamEvent.objects.exists())

    def test_retention_defaults_to_a_day(self):
        kept = make_event(age=timedelta(hours=23))
        make_event(age=timedelta(hours=25))
        call_command("prune_stream_events", stdout=StringIO())
        self.assertEqual(list(StreamEvent.objects.values_list("id", flat=True)), [kept.id])
