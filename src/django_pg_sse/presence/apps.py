"""The presence app: its checks and the receivers of the stream signals."""

from django.apps import AppConfig
from django.core import checks


class PresenceConfig(AppConfig):
    """Tracks the readers of PRESENCE_TOPICS through stream_opened and stream_closed."""

    name = "django_pg_sse.presence"
    # Not "presence": a project may have an app of that name.
    label = "django_pg_sse_presence"
    verbose_name = "Event stream presence"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        from django_pg_sse.presence.checks import check_presence_identity, check_presence_topics
        from django_pg_sse.presence.tracker import stream_closed_receiver, stream_opened_receiver
        from django_pg_sse.signals import stream_closed, stream_opened

        checks.register(check_presence_topics)
        checks.register(check_presence_identity)
        stream_opened.connect(stream_opened_receiver, dispatch_uid="pg_sse.presence.opened")
        stream_closed.connect(stream_closed_receiver, dispatch_uid="pg_sse.presence.closed")
