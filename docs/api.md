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

## Settings

`PG_SSE` in your settings is a dict. Its keys override these defaults.

| Key | Default | Meaning |
|---|---|---|
| `CAN_SUBSCRIBE` | `"django_pg_sse.permissions.can_subscribe"` | Dotted path to a function `(request, topic) -> bool` that decides who may read a topic. |
| `RETENTION_HOURS` | `24` | How long `prune_stream_events` keeps events, which is how far back `Last-Event-ID` reaches. |
| `STREAM_CONNECTIONS` | `4` | Connections per ASGI worker for the replay and revocation queries of opening streams. |
| `DATABASE` | `"default"` | The `DATABASES` alias of the hub's connections. It must reach the same database as `default`. |
| `QUERY_TIMEOUT` | `30` | Seconds after which a query of the hub gives up; a half-open connection would wait minutes. |

## System checks

```{eval-rst}
.. autofunction:: django_pg_sse.checks.check_postgresql
.. autofunction:: django_pg_sse.checks.check_stream_connections
.. autofunction:: django_pg_sse.checks.check_retention_hours
.. autofunction:: django_pg_sse.checks.check_query_timeout
.. autofunction:: django_pg_sse.checks.check_hub_database
.. autofunction:: django_pg_sse.checks.check_middleware
```

## Management command

`manage.py prune_stream_events [--hours N]` deletes events older than the retention.
`--hours` overrides `PG_SSE["RETENTION_HOURS"]` and must be a positive integer.
