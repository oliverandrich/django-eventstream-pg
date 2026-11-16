# AGENTS.md

Guidance for coding agents and contributors. Development setup and commit conventions are in
[CONTRIBUTING.md](CONTRIBUTING.md).

## What this project is

`django-eventstream-pg` pushes server-sent events from Django to browsers over PostgreSQL
`LISTEN`/`NOTIFY`. It stores every event, so a reconnecting client replays what it missed.

## Package layout

```
src/django_eventstream_pg/
├── messages.py      # SSE format, NOTIFY payloads, Message, Owner, Signal; no Django imports
├── publish.py       # broadcast(), close_streams(), the user_logged_out receiver
├── hub.py           # one listener per event loop; fans events out to the loop's streams
├── views.py         # the SSE endpoint and its authorisation
├── permissions.py   # the default can_subscribe() policy
├── conf.py          # the EVENTSTREAM_PG setting and its defaults
├── checks.py        # system check: the default database must be PostgreSQL
├── models.py        # StreamEvent, the stored events
└── management/commands/prune_stream_events.py
```

## Invariants

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
- An open stream holds no connection from Django's pool. The hub uses three connections of its own.
  `Hub.live` runs the queries of live delivery and of the catch-up.
  `Hub.streams` runs the view's replay and revocation queries.
  `views._messages()` closes the request thread's connection after the response middleware.
- The hub's connections bypass Django's connection setup, so `_connect()` applies `assume_role`.
- A stream sends event ids signed by `EventIds` for its owner's close topics and its topics.
  Replay needs a validly signed id and no close for the owner after that id.
- `__init__.py` must not import the model at module level. Django imports the package while it
  loads `INSTALLED_APPS`, before models can load.

## Tests

The suite needs a PostgreSQL server; `tests/settings.py` reads `POSTGRES_*` variables and
defaults to the local Unix socket. Hub tests run in the test's own event loop: decorate them with
`stops_the_hub`, because Django's test cases run no `asyncTearDown`.
