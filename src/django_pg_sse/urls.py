from django.urls import path

from django_pg_sse import views

app_name = "pg_sse"

urlpatterns = [
    path("", views.stream, name="stream"),
]
