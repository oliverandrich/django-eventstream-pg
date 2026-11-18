import contextlib
from typing import Any
from unittest import mock

from django.conf import settings
from django.core.checks.registry import registry
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

    def test_the_hub_database_must_be_a_configured_alias(self):
        with override_settings(PG_SSE={"DATABASE": "missing"}):
            errors = checks.check_hub_database(None)
        self.assertEqual([error.id for error in errors], ["pg_sse.E005"])
        with override_settings(PG_SSE={"DATABASE": {"NAME": "app"}}):
            errors = checks.check_hub_database(None) + checks.check_postgresql(None)
        self.assertEqual([error.id for error in errors], ["pg_sse.E005"])
        with override_settings(PG_SSE={"DATABASE": "direct"}):
            self.assertEqual(checks.check_hub_database(None), [])

    def test_a_missing_authentication_middleware_is_a_warning(self):
        middleware = [m for m in settings.MIDDLEWARE if not m.endswith("AuthenticationMiddleware")]
        with override_settings(MIDDLEWARE=middleware):
            warnings = checks.check_middleware(None)
        self.assertEqual([warning.id for warning in warnings], ["pg_sse.W001"])
        self.assertEqual(checks.check_middleware(None), [])


class ConnectionPoolCheckTest(SimpleTestCase):
    def pools(self, **pools: bool) -> contextlib.ExitStack:
        """Turn Django's connection pool on or off per alias for the duration of the block."""
        stack = contextlib.ExitStack()
        for alias, pool in pools.items():
            current: dict[str, Any] = connections[alias].settings_dict.get("OPTIONS") or {}
            options = {**current, "pool": pool}
            stack.enter_context(mock.patch.dict(connections[alias].settings_dict, OPTIONS=options))
        return stack

    def warnings(self) -> list[tuple[str, str]]:
        return [(warning.id or "", warning.msg) for warning in checks.check_connection_pool(None)]

    @override_settings(PG_SSE={})
    def test_default_without_a_pool_is_w002(self):
        with self.pools(default=False):
            ((check_id, message),) = self.warnings()
        self.assertEqual(check_id, "pg_sse.W002")
        self.assertIn("'default'", message)
        with self.pools(default=True):
            self.assertEqual(self.warnings(), [])

    @override_settings(PG_SSE={"DATABASE": "direct"})
    def test_the_hub_alias_without_a_pool_is_w003(self):
        # Behind PgBouncer, a project silences W002 for default and keeps W003 for the direct alias.
        with self.pools(default=True, direct=False):
            self.assertEqual([check_id for check_id, _ in self.warnings()], ["pg_sse.W003"])
        with self.pools(default=False, direct=False):
            self.assertEqual([check_id for check_id, _ in self.warnings()], ["pg_sse.W002", "pg_sse.W003"])
        with self.pools(default=True, direct=True):
            self.assertEqual(self.warnings(), [])

    @override_settings(PG_SSE={"DATABASE": "missing"})
    def test_an_undefined_hub_alias_is_left_to_e005(self):
        with self.pools(default=True):
            self.assertEqual(self.warnings(), [])

    @override_settings(PG_SSE={})
    def test_another_database_is_left_to_e001(self):
        with self.pools(default=False), mock.patch.object(connections["default"], "vendor", "sqlite"):
            self.assertEqual(self.warnings(), [])

    @override_settings(PG_SSE={})
    def test_options_set_to_none_count_as_no_pool(self):
        with mock.patch.dict(connections["default"].settings_dict, OPTIONS=None):
            self.assertEqual([check_id for check_id, _ in self.warnings()], ["pg_sse.W002"])

    def test_it_runs_with_the_deploy_checks_only(self):
        self.assertIn(checks.check_connection_pool, registry.get_checks(include_deployment_checks=True))
        self.assertNotIn(checks.check_connection_pool, registry.get_checks(include_deployment_checks=False))


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
