"""The stream endpoint. Include it under a path of your choice; its URL name is pg_sse:stream."""

from django.urls import path

from django_pg_sse import views

app_name = "pg_sse"

urlpatterns = [
    path("", views.stream, name="stream"),
]
