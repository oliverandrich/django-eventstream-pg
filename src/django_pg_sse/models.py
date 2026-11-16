# Field annotations name stub-only generics, so they must stay unevaluated at runtime.
from __future__ import annotations

from datetime import datetime

from django.db import models


class StreamEvent(models.Model):
    """A server-sent event, kept for replay after a reconnect."""

    id: models.BigAutoField[int, int] = models.BigAutoField(primary_key=True)
    topic: models.CharField[str, str] = models.CharField(max_length=200)
    event: models.CharField[str, str] = models.CharField(max_length=100, blank=True)
    data: models.TextField[str, str] = models.TextField()
    created: models.DateTimeField[datetime, datetime] = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        indexes = [models.Index(fields=["topic", "id"], name="django_pg_sse_topic_id")]

    def __str__(self) -> str:
        return f"{self.topic} #{self.id}"
