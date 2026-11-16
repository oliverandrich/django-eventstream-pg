import importlib
import inspect
import pkgutil
import re
from pathlib import Path

from django.test import SimpleTestCase

import django_pg_sse
from django_pg_sse import conf, hub, signals, views


class DocstringTest(SimpleTestCase):
    """The API reference renders from docstrings, so every public name needs one."""

    def test_every_module_has_a_docstring(self):
        names = [info.name for info in pkgutil.walk_packages(django_pg_sse.__path__, "django_pg_sse.")]
        missing = [
            name for name in names if ".migrations" not in name and not inspect.getdoc(importlib.import_module(name))
        ]
        self.assertEqual(missing, [])

    def test_every_public_name_has_a_docstring(self):
        public = [getattr(django_pg_sse, name) for name in django_pg_sse.__all__]
        public += [signals.StreamInfo]
        missing = [item.__name__ for item in public if not inspect.getdoc(item)]
        self.assertEqual(missing, [])


class SettingsPageTest(SimpleTestCase):
    """docs/settings.md is the one page that lists every PG_SSE key and the fixed values."""

    page = (Path(__file__).parents[1] / "docs" / "settings.md").read_text()

    def sections(self, level: str) -> dict[str, str]:
        """The page's sections at `level`, such as "### ", by their heading."""
        parts = re.split(rf"^{level}(.+)$", self.page, flags=re.MULTILINE)
        return {heading.strip("` "): body for heading, body in zip(parts[1::2], parts[2::2], strict=True)}

    def test_every_setting_is_documented_with_its_default(self):
        pg_sse = self.sections("## ")["PG_SSE"]
        keys = re.findall(r"^### `(\w+)`$", pg_sse, flags=re.MULTILINE)
        self.assertEqual(sorted(keys), sorted(conf.DEFAULTS))
        sections = self.sections("### ")
        for key, default in conf.DEFAULTS.items():
            expected = f"Default: `{default!r}`".replace("'", '"')
            self.assertIn(expected, sections[key], key)

    def test_the_fixed_values_match_the_code(self):
        delays = ", ".join(f"{delay:g}" for delay in hub.RECONNECT_DELAYS)
        for value in (
            f"| {views.KEEPALIVE_SECONDS} seconds |",
            f"| {views.READY_TIMEOUT} seconds |",
            f"| {hub.HEALTH_SECONDS} + {hub.HEALTH_TIMEOUT} seconds |",
            f"| {delays} seconds |",
            f"| {hub.QUEUE_SIZE} events |",
            f"| {hub.REPLAY_BATCH} events |",
        ):
            self.assertIn(value, self.page, value)

    def test_the_libpq_defaults_match_the_code(self):
        rows = {**hub.SOCKET_OPTIONS, "connect_timeout": hub.CONNECT_TIMEOUT}
        for option, value in rows.items():
            self.assertRegex(self.page, rf"\| `{option}` \| `{value}`", option)
