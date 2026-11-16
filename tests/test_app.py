from unittest import mock

from django.core.management import call_command
from django.db import connections
from django.http import HttpRequest
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings

from django_eventstream_pg import checks, conf


def only_room_one(request: HttpRequest, topic: str) -> bool:
    return topic == "room:1"


class CanSubscribeSettingTest(SimpleTestCase):
    def test_defaults_to_the_bundled_policy(self):
        from django_eventstream_pg.permissions import can_subscribe

        self.assertIs(conf.can_subscribe(), can_subscribe)

    @override_settings(EVENTSTREAM_PG={"CAN_SUBSCRIBE": "tests.test_app.only_room_one"})
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
        self.assertEqual([error.id for error in errors], ["eventstream_pg.E001"])

    def test_stream_connections_must_be_a_positive_integer(self):
        for value in (0, -1, "4"):
            with override_settings(EVENTSTREAM_PG={"STREAM_CONNECTIONS": value}):
                errors = checks.check_stream_connections(None)
            self.assertEqual([error.id for error in errors], ["eventstream_pg.E002"], value)
        self.assertEqual(checks.check_stream_connections(None), [])


class MigrationsTest(TestCase):
    def test_models_and_migrations_agree(self):
        # makemigrations --check exits non-zero when a model change lacks a migration.
        call_command("makemigrations", "eventstream_pg", "--check", "--dry-run", verbosity=0)


class PublicApiTest(SimpleTestCase):
    def test_package_exports_broadcast_and_close_streams(self):
        import django_eventstream_pg
        from django_eventstream_pg import publish

        self.assertIs(django_eventstream_pg.broadcast, publish.broadcast)
        self.assertIs(django_eventstream_pg.close_streams, publish.close_streams)
