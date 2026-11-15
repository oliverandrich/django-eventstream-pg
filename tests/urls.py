from django.urls import include, path

urlpatterns = [
    path("events/", include("django_eventstream_pg.urls")),
]
