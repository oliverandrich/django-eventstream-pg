from django.apps import AppConfig, apps
from django.core import checks


class PgSseConfig(AppConfig):
    # The label stays the module name: PostgreSQL treats names starting with pg_ as system names.
    name = "django_pg_sse"
    verbose_name = "Event stream"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        from django_pg_sse.checks import check_postgresql, check_stream_connections

        checks.register(check_postgresql)
        checks.register(check_stream_connections)
        if apps.is_installed("django.contrib.auth"):
            from django.contrib.auth.signals import user_logged_out

            from django_pg_sse.publish import close_session_streams

            user_logged_out.connect(close_session_streams, dispatch_uid="pg_sse.close_session_streams")
