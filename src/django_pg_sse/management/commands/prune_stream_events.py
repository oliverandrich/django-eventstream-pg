"""The prune_stream_events management command."""

from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.utils import timezone

from django_pg_sse import conf
from django_pg_sse.checks import is_positive_int
from django_pg_sse.models import StreamEvent


class Command(BaseCommand):
    """manage.py prune_stream_events [--hours N]: delete events older than the retention."""

    help = "Delete stream events older than the retention, which is how far back Last-Event-ID reaches."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--hours", type=int, help="Retention in hours; defaults to PG_SSE['RETENTION_HOURS'].")

    def handle(self, *_args: Any, **options: Any) -> None:
        hours = options["hours"] if options["hours"] is not None else conf.get("RETENTION_HOURS")
        # Zero or a negative value puts the cutoff at or after now, and current events go too.
        if not is_positive_int(hours):
            raise CommandError(f"The retention must be a positive number of hours, not {hours!r}.")
        deleted, _ = StreamEvent.objects.filter(created__lt=timezone.now() - timedelta(hours=hours)).delete()
        if options["verbosity"] > 0:
            self.stdout.write(f"Deleted {deleted} stream events older than {hours} hours.")
