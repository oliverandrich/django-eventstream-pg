# API reference

## Sending events

```{eval-rst}
.. autofunction:: django_pg_sse.broadcast
.. autofunction:: django_pg_sse.broadcast_json
.. autofunction:: django_pg_sse.close_streams
.. autofunction:: django_pg_sse.abroadcast
.. autofunction:: django_pg_sse.abroadcast_json
.. autofunction:: django_pg_sse.aclose_streams
```

## Who may read a topic

```{eval-rst}
.. autofunction:: django_pg_sse.permissions.can_subscribe
```

## Signals

```{eval-rst}
.. autodata:: django_pg_sse.signals.stream_opened
   :annotation:
.. autodata:: django_pg_sse.signals.stream_closed
   :annotation:
.. autoclass:: django_pg_sse.signals.StreamInfo
   :members:
```

## Presence

```{eval-rst}
.. autoclass:: django_pg_sse.presence.Presence
   :members:
.. autofunction:: django_pg_sse.presence.tracker.present
.. autofunction:: django_pg_sse.presence.tracker.apresent
.. autodata:: django_pg_sse.presence.presence_changed
   :annotation:
.. autofunction:: django_pg_sse.presence.user_identity
```

## Settings

[Settings](settings.md) lists every `PG_SSE` key with its default, and the fixed values.

## System checks

```{eval-rst}
.. autofunction:: django_pg_sse.checks.check_postgresql
.. autofunction:: django_pg_sse.checks.check_stream_connections
.. autofunction:: django_pg_sse.checks.check_retention_hours
.. autofunction:: django_pg_sse.checks.check_query_timeout
.. autofunction:: django_pg_sse.checks.check_hub_database
.. autofunction:: django_pg_sse.checks.check_middleware
.. autofunction:: django_pg_sse.checks.check_connection_pool
.. autofunction:: django_pg_sse.presence.checks.check_presence_topics
.. autofunction:: django_pg_sse.presence.checks.check_presence_identity
```

## Management command

`manage.py prune_stream_events [--hours N]` deletes events older than the retention.
`--hours` overrides `PG_SSE["RETENTION_HOURS"]` and must be a positive integer.
