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
- `close_streams()` stores its targets as events on the topic `_close`. They replay like any
  event, so a listener that reconnects still applies them.
- A session target holds the SHA-256 digest of the session key, never the key itself.
  The view refuses `_close` before `CAN_SUBSCRIBE` runs.
- An open stream holds no connection from Django's pool. The hub uses two connections of its own.
  `views._messages()` closes the request thread's connection after the response middleware.
- The hub's connections bypass Django's connection setup, so `_connect()` applies `assume_role`.
- `__init__.py` must not import the model at module level. Django imports the package while it
  loads `INSTALLED_APPS`, before models can load.

## Tests

The suite needs a PostgreSQL server; `tests/settings.py` reads `POSTGRES_*` variables and
defaults to the local Unix socket. Hub tests run in the test's own event loop: decorate them with
`stops_the_hub`, because Django's test cases run no `asyncTearDown`.
