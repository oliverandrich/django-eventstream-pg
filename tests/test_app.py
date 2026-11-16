from unittest import mock

from django.conf import settings
from django.core.management import call_command
from django.db import connections
from django.http import HttpRequest
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings

from django_pg_sse import checks, conf


def only_room_one(request: HttpRequest, topic: str) -> bool:
    return topic == "room:1"


class CanSubscribeSettingTest(SimpleTestCase):
    def test_defaults_to_the_bundled_policy(self):
        from django_pg_sse.permissions import can_subscribe

        self.assertIs(conf.can_subscribe(), can_subscribe)

    @override_settings(PG_SSE={"CAN_SUBSCRIBE": "tests.test_app.only_room_one"})
    def test_the_setting_names_another_function(self):
        check = conf.can_subscribe()
        request = RequestFactory().get("/")
        self.assertTrue(check(request, "room:1"))
        self.assertFalse(check(request, "public:news"))


class CheckTest(SimpleTestCase):
    def test_postgresql_passes(self):
        self.assertEqual(checks.check_postgresql(None), [])

    def test_another_database_is_an_error(self):
        with mock.patch.object(connections["default"], "vendor", "sqlite"):
            errors = checks.check_postgresql(None)
        self.assertEqual([error.id for error in errors], ["pg_sse.E001"])

    def test_stream_connections_must_be_a_positive_integer(self):
        for value in (0, -1, "4"):
            with override_settings(PG_SSE={"STREAM_CONNECTIONS": value}):
                errors = checks.check_stream_connections(None)
            self.assertEqual([error.id for error in errors], ["pg_sse.E002"], value)
        self.assertEqual(checks.check_stream_connections(None), [])

    def test_retention_hours_must_be_a_positive_integer(self):
        # A negative retention puts the cutoff in the future, and pruning deletes current events.
        for value in (0, -1, "24"):
            with override_settings(PG_SSE={"RETENTION_HOURS": value}):
                errors = checks.check_retention_hours(None)
            self.assertEqual([error.id for error in errors], ["pg_sse.E003"], value)
        self.assertEqual(checks.check_retention_hours(None), [])

    def test_query_timeout_must_be_a_positive_number(self):
        for value in (0, -1, "30", True):
            with override_settings(PG_SSE={"QUERY_TIMEOUT": value}):
                errors = checks.check_query_timeout(None)
            self.assertEqual([error.id for error in errors], ["pg_sse.E004"], value)
        for value in (0.5, 30):
            with override_settings(PG_SSE={"QUERY_TIMEOUT": value}):
                self.assertEqual(checks.check_query_timeout(None), [], value)

    def test_a_missing_authentication_middleware_is_a_warning(self):
        middleware = [m for m in settings.MIDDLEWARE if not m.endswith("AuthenticationMiddleware")]
        with override_settings(MIDDLEWARE=middleware):
            warnings = checks.check_middleware(None)
        self.assertEqual([warning.id for warning in warnings], ["pg_sse.W001"])
        self.assertEqual(checks.check_middleware(None), [])


class MigrationsTest(TestCase):
    def test_models_and_migrations_agree(self):
        # makemigrations --check exits non-zero when a model change lacks a migration.
        call_command("makemigrations", "django_pg_sse", "--check", "--dry-run", verbosity=0)


class PublicApiTest(SimpleTestCase):
    def test_package_exports_its_public_functions(self):
        import django_pg_sse
        from django_pg_sse import publish

        self.assertIs(django_pg_sse.broadcast, publish.broadcast)
        self.assertIs(django_pg_sse.close_streams, publish.close_streams)
        self.assertIs(django_pg_sse.broadcast_json, publish.broadcast_json)
        self.assertIs(django_pg_sse.abroadcast, publish.abroadcast)
        self.assertIs(django_pg_sse.abroadcast_json, publish.abroadcast_json)
        self.assertIs(django_pg_sse.aclose_streams, publish.aclose_streams)
