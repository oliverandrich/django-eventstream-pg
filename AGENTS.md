# AGENTS.md

Guidance for coding agents and contributors. Development setup and commit conventions are in
[CONTRIBUTING.md](CONTRIBUTING.md).

## What this project is

`django-pg-sse` pushes server-sent events from Django to browsers over PostgreSQL
`LISTEN`/`NOTIFY`. It stores every event, so a reconnecting client replays what it missed.

## Package layout

```
src/django_pg_sse/
├── messages.py      # SSE format, NOTIFY payloads, Message, Owner, Signal; no Django imports
├── publish.py       # broadcast(), broadcast_json(), close_streams(), the user_logged_out receiver
├── hub.py           # one listener per event loop; fans events out to the loop's streams
├── views.py         # the SSE endpoint and its authorisation
├── signals.py       # stream_opened, stream_closed and their StreamInfo
├── permissions.py   # the default can_subscribe() policy
├── conf.py          # the PG_SSE setting and its defaults
├── checks.py        # system check: the default database must be PostgreSQL
├── models.py        # StreamEvent, the stored events
└── management/commands/prune_stream_events.py
```

## Invariants

- `Hub._listen()` reads the newest id before `LISTEN` on first start, so no commit falls between.
  `_catch_up()` replays up to the newest id and then sets `last_id` to it, read topics or not.
  A stream subscribing meanwhile then sees `last_id` past `authorized_at` and checks for closes.
- A LISTEN connection quiet for `HEALTH_SECONDS` must answer `SELECT 1` within `HEALTH_TIMEOUT`.
  Otherwise `_listen()` treats it as lost and reconnects.
  psycopg then waits up to 10 seconds more for the cancelled query before the connection closes.
- `_Connection.fetch()` and `_Pool.fetch()` retry once when an `OperationalError` closed the connection.
  `_Pool.fetch()` first runs `check()`, which drops every closed idle connection of the pool.
  A failed connect and other errors, such as SQL errors, are not retried.
- `broadcast()` writes after the commit, in a short transaction holding an advisory lock.
  Event ids therefore become visible in order. Replay after `Last-Event-ID` depends on it.
- `_publish()` runs one statement: it takes the lock, stores the event and sends the NOTIFY.
  The INSERT reads from the lock's CTE, so the event takes its id under the lock.
- Topics starting with `_` are reserved. The view refuses them before `CAN_SUBSCRIBE` runs,
  and `broadcast()` raises `ValueError`. Package code writes them through `_broadcast()`.
- `close_streams()` stores an event on each close topic of the owner, `_close:<kind>:<sha256>`.
  The hub indexes close topics like any topic, so a listener that reconnects still applies them.
- The digest keeps the session key out of the table and fits any primary key into the column.
- `Hub._listen()` passes every NOTIFY already received to `_receive()` at once.
  `_receive()` loads their events with `id = ANY`, again for streams that subscribed meanwhile.
  It delivers without an await in between and advances `last_id` only afterwards.
- An open stream holds no connection from Django's pool. The hub uses connections of its own.
  `Hub.live` runs the queries of live delivery and of the catch-up.
  `Hub.streams` pools up to `STREAM_CONNECTIONS` for the view's replay and revocation queries.
  `views._messages()` closes the request thread's connection after the response middleware.
- The hub's connections bypass Django's connection setup, so `_connect()` applies `assume_role`.
  `connect_params()` uses `get_connection_params()`, which drops `pool`, `isolation_level` and
  `server_side_binding`. It also drops Django's `cursor_factory` and `context`.
- A stream sends event ids signed by `EventIds` for its owner's close topics and its topics.
  Replay needs a validly signed id and no close for the owner after that id.
- `stream_opened` and `stream_closed` come in pairs: `_messages()` sends closed once opened began.
  The closed send runs under `asyncio.shield`, so a second cancellation cannot cut it short.
  Receivers run through `asend_robust()`, so a failing receiver is logged and the stream goes on.
  `_send()` closes the request thread's connection again, which a sync receiver may have opened.
- `__init__.py` must not import the model at module level. Django imports the package while it
  loads `INSTALLED_APPS`, before models can load.

## Tests

The suite needs a PostgreSQL server; `tests/settings.py` reads `POSTGRES_*` variables and
defaults to the local Unix socket. Hub tests run in the test's own event loop: decorate them with
`stops_the_hub`, because Django's test cases run no `asyncTearDown`.
