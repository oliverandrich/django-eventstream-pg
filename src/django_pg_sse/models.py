"""StreamEvent, the stored events that replay reads."""

# Field annotations name stub-only generics, so they must stay unevaluated at runtime.
from __future__ import annotations

from datetime import datetime

from django.db import models

TOPIC_MAX_LENGTH = 200
EVENT_MAX_LENGTH = 100
LATEST_MAX_LENGTH = 100


class StreamEvent(models.Model):
    """A server-sent event, kept for replay after a reconnect."""

    id: models.BigAutoField[int, int] = models.BigAutoField(primary_key=True)
    topic: models.CharField[str, str] = models.CharField(max_length=TOPIC_MAX_LENGTH)
    event: models.CharField[str, str] = models.CharField(max_length=EVENT_MAX_LENGTH, blank=True)
    data: models.TextField[str, str] = models.TextField()
    #: A key for snapshot events: replay skips an event when a newer one has the same topic and key.
    #: db_default: workers of an older release write events without the column during a deploy.
    latest: models.CharField[str, str] = models.CharField(
        max_length=LATEST_MAX_LENGTH, blank=True, default="", db_default=""
    )
    created: models.DateTimeField[datetime, datetime] = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["topic", "id"], name="django_pg_sse_topic_id"),
            # Only snapshot events take part in the replay's search for a newer one.
            models.Index(fields=["topic", "latest", "id"], name="django_pg_sse_latest", condition=~models.Q(latest="")),
        ]

    def __str__(self) -> str:
        return f"{self.topic} #{self.id}"
