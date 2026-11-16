# django-pg-sse

Server-sent events for Django over PostgreSQL `LISTEN`/`NOTIFY`. Push HTML from any process to the
open browsers of a topic, with replay after a reconnect. No Redis, no message broker.

Built for [htmx 4](https://four.htmx.org) and its `hx-sse` extension, but any SSE client works.

## Requirements

- Python 3.12 or newer, Django 5.2 or newer
- PostgreSQL and psycopg 3
- An ASGI server such as granian, uvicorn or daphne. `runserver` speaks WSGI and cannot stream an
  async response.

## Installation

```shell
uv add django-pg-sse
```

```python
INSTALLED_APPS = [
    # ...
    "django_pg_sse",
]
```

```python
# urls.py
urlpatterns = [
    # ...
    path("events/", include("django_pg_sse.urls")),
]
```

Run `manage.py migrate` to create the event table.

## Usage

Send HTML to a topic. The event goes out when the current transaction commits; a rollback sends
nothing.

```python
from django_pg_sse import broadcast

html = '<ul id="messages" hx-swap-oob="beforeend"><li id="message-7">Hi</li></ul>'
broadcast(f"room:{room.pk}", html)
```

A page opens one stream for all its topics:

```html
<ul id="messages"></ul>
<div hx-sse:connect="{% url 'pg_sse:stream' %}?topic=room:{{ room.pk }}&topic=user:{{ request.user.pk }}"></div>
```

htmx swaps each message. Elements with `hx-swap-oob` or `<hx-partial>` update the page anywhere.
Give list items stable ids, so an event delivered twice replaces its element instead of adding one.

## Who may read a topic

`PG_SSE["CAN_SUBSCRIBE"]` names a function `(request, topic) -> bool`. The default opens
`public:` topics to everyone and `user:<pk>` to that user, and closes every other kind.
Topics starting with `_` belong to the package. The view refuses them before the check runs,
and `broadcast()` raises `ValueError` for them.

```python
PG_SSE = {"CAN_SUBSCRIBE": "myproject.events.can_subscribe"}
```

```python
from django_pg_sse.permissions import can_subscribe as default


def can_subscribe(request, topic):
    kind, _, key = topic.partition(":")
    if kind == "room":
        return request.user.is_authenticated and Membership.objects.filter(room=key, user=request.user).exists()
    return default(request, topic)
```

The check runs when a stream opens. After taking access away, call `close_streams()`:

```python
from django_pg_sse import close_streams

close_streams(user=user)  # every stream of the user
close_streams(session=request.session)  # the streams of one session
```

Each stream receives an event named `closed` and ends. htmx reconnects, and the check runs again.
Add `hx-sse:close="closed"` to a stream element that should stop instead. Logging out closes the
streams of that session, so other tabs lose the user's topics.

## Replay

Every event is stored. A reconnecting browser sends `Last-Event-ID` and receives what it missed.
Event ids are signed with `SECRET_KEY` for the user, the session and the topics of the stream.
An unsigned id, or one signed for another stream, replays nothing; the stream starts with live events.
A `close_streams()` for the owner after the id also skips the replay.
The events from the time without access then stay unread.
Run `manage.py prune_stream_events` regularly, for example hourly; it deletes events older than
`PG_SSE["RETENTION_HOURS"]` (default 24).

## Database connections

The hub's connections use `DATABASES["default"]`, outside Django's connection pool.
Each ASGI worker holds one for LISTEN, one for live delivery and a pool for opening streams.
`PG_SSE["STREAM_CONNECTIONS"]` (default 4) caps that pool.
A worker therefore holds up to 2 + `STREAM_CONNECTIONS` connections.

- They take its host, port, user, password and the libpq `OPTIONS`, such as `sslmode`, `service`,
  `passfile`, `application_name` and `prepare_threshold`.
- They apply `OPTIONS["assume_role"]` with `SET ROLE`.
- `pool`, `isolation_level` and `server_side_binding` do not apply. The hub runs in autocommit.
- LISTEN needs a session. A PgBouncer in transaction mode cannot carry it.
  With such a PgBouncer, point `default` straight at PostgreSQL.

## How it works

- `broadcast()` writes the event after the commit, in a short transaction that holds an advisory
  lock. Event ids therefore become visible in order, which keeps the replay exact.
- `NOTIFY` carries only the id and the topic, so the 8000-byte payload limit does not apply.
- The number of connections per ASGI worker does not grow with the number of open streams.
  Open streams hold no connection from Django's pool.
- A listener that loses its connection reconnects and loads the events it missed from the table.
- The view opts out of `ATOMIC_REQUESTS`, which Django does not allow for async views.
- `close_streams()` stores an event on a close topic of the user or the session.
  Only a process with a stream of that owner acts on it, and it needs no query.
- A close topic holds a SHA-256 digest of the user's primary key or the session key.

## Compared to django-eventstream

[django-eventstream](https://github.com/fanout/django-eventstream) also serves server-sent events
from Django. It shares no code or API with this package. This comparison refers to its release 5.3.4.

django-pg-sse supports PostgreSQL only and ASGI only. In return it needs no other service.

| | django-pg-sse | django-eventstream |
|---|---|---|
| Delivery across processes | PostgreSQL `LISTEN`/`NOTIFY` | Redis pub/sub or a GRIP proxy such as Pushpin; otherwise the sending process only |
| Sending | After the transaction commits; a rollback sends nothing | At once, without `on_commit` |
| Storage | Every event | Optional, per channel |
| Event ids | One sequence, in commit order across all topics | One counter per channel |
| `Last-Event-ID` | Signed for the user, the session and the topics | Chosen by the client |
| Revoking access | `close_streams()` for a user or a session, in every process | `channel_permission_changed()` for a user and a channel, in the calling process and at a GRIP proxy |
| Payload | HTML for htmx, or any text | JSON by default, or any text |
| Servers | ASGI | ASGI, or WSGI behind a GRIP proxy |

django-eventstream with Pushpin or Fanout holds the open connections outside Django.
That scales to more concurrent connections than an ASGI worker holds.
Without such a proxy, both packages hold each open stream in an ASGI worker.

django-pg-sse serialises `broadcast()` through an advisory lock, so the events per second have a
lower ceiling than with Redis.

## License

MIT
