from django.apps import AppConfig, apps
from django.core import checks


class EventStreamPgConfig(AppConfig):
    name = "django_eventstream_pg"
    label = "eventstream_pg"
    verbose_name = "Event stream"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        from django_eventstream_pg.checks import check_postgresql, check_stream_connections

        checks.register(check_postgresql)
        checks.register(check_stream_connections)
        if apps.is_installed("django.contrib.auth"):
            from django.contrib.auth.signals import user_logged_out

            from django_eventstream_pg.publish import close_session_streams

            user_logged_out.connect(close_session_streams, dispatch_uid="eventstream_pg.close_session_streams")
