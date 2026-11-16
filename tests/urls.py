from django.urls import include, path

urlpatterns = [
    path("events/", include("django_pg_sse.urls")),
]
