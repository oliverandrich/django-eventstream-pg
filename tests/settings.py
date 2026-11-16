import os
from typing import Any

DEBUG = False
SECRET_KEY = "NOTASECRET"
ALLOWED_HOSTS = ["*"]
USE_TZ = True

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django_pg_sse",
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
]

ROOT_URLCONF = "tests.urls"

# Empty values make psycopg use the local Unix socket as the current user.
DATABASES: dict[str, dict[str, Any]] = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("POSTGRES_DB", "django_pg_sse"),
        "USER": os.environ.get("POSTGRES_USER", ""),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
        "HOST": os.environ.get("POSTGRES_HOST", ""),
        "PORT": os.environ.get("POSTGRES_PORT", ""),
    },
}
# The same database under a second alias, as for a hub that bypasses PgBouncer.
# application_name tells its connections apart from those of default.
DATABASES["direct"] = {
    **DATABASES["default"],
    "OPTIONS": {"application_name": "pg_sse_direct"},
    "TEST": {"MIRROR": "default"},
}
