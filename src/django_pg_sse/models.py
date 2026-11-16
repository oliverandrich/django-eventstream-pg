"""StreamEvent, the stored events that replay reads."""

# Field annotations name stub-only generics, so they must stay unevaluated at runtime.
from __future__ import annotations

from datetime import datetime

from django.db import models

TOPIC_MAX_LENGTH = 200
EVENT_MAX_LENGTH = 100


class StreamEvent(models.Model):
    """A server-sent event, kept for replay after a reconnect."""

    id: models.BigAutoField[int, int] = models.BigAutoField(primary_key=True)
    topic: models.CharField[str, str] = models.CharField(max_length=TOPIC_MAX_LENGTH)
    event: models.CharField[str, str] = models.CharField(max_length=EVENT_MAX_LENGTH, blank=True)
    data: models.TextField[str, str] = models.TextField()
    created: models.DateTimeField[datetime, datetime] = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        indexes = [models.Index(fields=["topic", "id"], name="django_pg_sse_topic_id")]

    def __str__(self) -> str:
        return f"{self.topic} #{self.id}"
