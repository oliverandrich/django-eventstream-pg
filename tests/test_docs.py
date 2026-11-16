import importlib
import inspect
import pkgutil

from django.test import SimpleTestCase

import django_pg_sse
from django_pg_sse import signals


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
