"""Sphinx configuration for the django-pg-sse documentation."""

import django
from django.conf import settings

# autodoc imports the package, whose modules need configured settings and loaded apps.
# No database connection opens while the docs build.
settings.configure(
    INSTALLED_APPS=["django.contrib.auth", "django.contrib.contenttypes", "django_pg_sse"],
    DATABASES={"default": {"ENGINE": "django.db.backends.postgresql", "NAME": "docs"}},
)
django.setup()

project = "django-pg-sse"
copyright = "2026, Oliver Andrich"  # noqa: A001
author = "Oliver Andrich"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx_copybutton",
]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# The docstrings mark code with single backticks, as Markdown does.
default_role = "code"

autodoc_member_order = "bysource"
autodoc_typehints = "description"

html_theme = "furo"
html_title = "django-pg-sse"
html_theme_options = {
    "source_repository": "https://github.com/oliverandrich/django-pg-sse",
    "source_branch": "main",
    "source_directory": "docs/",
}

myst_enable_extensions = ["colon_fence", "deflist"]
# The README links to its sections, such as #who-may-read-a-topic.
myst_heading_anchors = 3

source_suffix = {".md": "markdown"}
