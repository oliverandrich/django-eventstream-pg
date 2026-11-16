"""The app configuration: system checks and the logout receiver."""

from django.apps import AppConfig, apps
from django.core import checks


class PgSseConfig(AppConfig):
    """Registers the system checks and, with django.contrib.auth, closes streams on logout."""

    # The label stays the module name: PostgreSQL treats names starting with pg_ as system names.
    name = "django_pg_sse"
    verbose_name = "Event stream"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        from django_pg_sse.checks import (
            check_hub_database,
            check_middleware,
            check_postgresql,
            check_query_timeout,
            check_retention_hours,
            check_stream_connections,
        )

        for check in (
            check_postgresql,
            check_hub_database,
            check_stream_connections,
            check_retention_hours,
            check_query_timeout,
            check_middleware,
        ):
            checks.register(check)
        if apps.is_installed("django.contrib.auth"):
            from django.contrib.auth.signals import user_logged_out

            from django_pg_sse.publish import close_session_streams

            user_logged_out.connect(close_session_streams, dispatch_uid="pg_sse.close_session_streams")
