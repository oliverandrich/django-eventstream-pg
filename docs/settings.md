# Settings

## `PG_SSE`

`PG_SSE` in your settings is a dict.
Each key you set overrides its default; the others keep theirs.

```python
PG_SSE = {
    "CAN_SUBSCRIBE": "myproject.events.can_subscribe",
    "RETENTION_HOURS": 48,
}
```

### `CAN_SUBSCRIBE`

Default: `"django_pg_sse.permissions.can_subscribe"`

The dotted path to a function `(request, topic) -> bool`.
It decides whether the request may read a topic, when a stream opens.
The default opens `public:` topics to everyone and `user:<pk>` to that user.
See [Who may read a topic](index.md#who-may-read-a-topic).

### `RETENTION_HOURS`

Default: `24`

How many hours `prune_stream_events` keeps an event.
`prune_stream_events --hours N` overrides it for one run.
A reconnecting browser replays missed events from this window.
It must be a positive integer; the system check `pg_sse.E003` reports any other value.

### `STREAM_CONNECTIONS`

Default: `4`

The size of each worker's pool for the replay and revocation queries of opening streams.
A worker holds up to 2 + `STREAM_CONNECTIONS` connections; see [Deployment](deployment.md#connections).
With its own `DATABASE` alias, the hub opens two more for a moment while it compares the alias with `default`.
It must be a positive integer; the system check `pg_sse.E002` reports any other value.

### `QUERY_TIMEOUT`

Default: `30`

The seconds after which a query on the hub's live connection or stream pool gives up.
psycopg can add up to 10 seconds while it cancels the query.
The same limit bounds the pool's check of its connections and the comparison of `DATABASE` with `default`.
It must be a positive number; the system check `pg_sse.E004` reports any other value.
See [Failures](deployment.md#failures).

### `DATABASE`

Default: `"default"`

The `DATABASES` alias of the hub's own connections.
It must reach the same database as `default`, which `broadcast()` writes to.
Use it to bypass a PgBouncer in transaction mode; see [PgBouncer](deployment.md#pgbouncer).
The system check `pg_sse.E005` reports an alias that `DATABASES` does not define.
`pg_sse.E001` reports an alias that is not PostgreSQL.

## Other settings that apply

### `DATABASES`

The hub's connections take host, port, user, password and the libpq `OPTIONS` of their alias.
They also apply `OPTIONS["assume_role"]`.
`pool`, `isolation_level` and `server_side_binding` do not apply; the hub runs in autocommit.
They add these libpq defaults, which a key in `OPTIONS` replaces:

| Option | Default |
|---|---|
| `connect_timeout` | `10`, unless `PGCONNECT_TIMEOUT`, `PGSERVICE` or `OPTIONS["service"]` is set |
| `keepalives` | `1` |
| `keepalives_idle` | `30` |
| `keepalives_interval` | `10` |
| `keepalives_count` | `3` |
| `tcp_user_timeout` | `30000` (milliseconds; Linux only) |

### `SECRET_KEY` and `SECRET_KEY_FALLBACKS`

`Last-Event-ID` values are signed with `SECRET_KEY`.
A key in `SECRET_KEY_FALLBACKS` keeps the ids signed with it valid; see [Security](deployment.md#security).

### `MIDDLEWARE`

The stream view and the default `CAN_SUBSCRIBE` read `request.user`.
The system check `pg_sse.W001` warns without `AuthenticationMiddleware` or a subclass of it.
It also warns when `django.contrib.auth` is not installed, whatever sets `request.user`.
A session is optional; without one, `close_streams(session=...)` reaches no stream.

## Fixed values

These values are part of the package's behaviour and cannot be set.

| Value | Meaning |
|---|---|
| 15 seconds | A stream sends a keepalive comment after this long without output. |
| 60 seconds | A new stream whose hub has not listened for this long ends. |
| 30 + 10 seconds | A quiet `LISTEN` connection gets a `SELECT 1` after 30 seconds and must answer within 10. |
| 0.5, 1, 2, 5 seconds | The delays between the listener's reconnect attempts; after these, it retries every 5 seconds without limit. |
| 1000 events | A stream's queue; a client that falls further behind is disconnected and replays. |
| 500 events | One replay batch; a longer replay ends the stream, and the reconnect fetches the next batch. |
