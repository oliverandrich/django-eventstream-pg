"""The rows behind presence: one per open stream and tracked topic."""

# Field annotations name stub-only generics, so they must stay unevaluated at runtime.
from __future__ import annotations

from datetime import datetime
from typing import Any

from django.db import models

from django_pg_sse.models import TOPIC_MAX_LENGTH


class PresenceEntry(models.Model):
    """One stream's reader on one topic. The table is UNLOGGED; the heartbeat refills it."""

    #: StreamInfo.id of the stream.
    stream: models.CharField[str, str] = models.CharField(max_length=32)
    topic: models.CharField[str, str] = models.CharField(max_length=TOPIC_MAX_LENGTH)
    identity: models.CharField[str, str] = models.CharField(max_length=255)
    meta: models.JSONField[dict[str, Any], dict[str, Any]] = models.JSONField(default=dict)
    #: Set to the database's now() on open and on every heartbeat.
    last_seen: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    #: Set when the stream closes; the row stays for the grace period.
    closed_at: models.DateTimeField[datetime | None, datetime | None] = models.DateTimeField(null=True)

    class Meta:
        db_table = "django_pg_sse_presence"
        constraints = [models.UniqueConstraint(fields=["stream", "topic"], name="django_pg_sse_presence_stream_topic")]
        indexes = [models.Index(fields=["topic", "identity"], name="django_pg_sse_presence_who")]

    def __str__(self) -> str:
        return f"{self.identity} on {self.topic}"
