from django.urls import path

from django_eventstream_pg import views

app_name = "eventstream_pg"

urlpatterns = [
    path("", views.stream, name="stream"),
]
