# Presence

`django_pg_sse.presence` tracks who has a topic open, such as the members of a chat room.
A reader counts once, however many tabs it has open.
It is optional and adds one table.

## Setup

```python
INSTALLED_APPS = [
    # ...
    "django_pg_sse",
    "django_pg_sse.presence",
]

PG_SSE = {
    "PRESENCE_TOPICS": ["room:"],
}
```

Run `manage.py migrate` to create the table `django_pg_sse_presence`.
Only streams on topics that start with a prefix in `PRESENCE_TOPICS` are tracked.

## Identity

`PRESENCE_IDENTITY` names a function `(request) -> (identity, meta) | None`.
It runs once when a stream opens on a presence topic.
The identity tells readers apart; streams with the same identity count as one reader.
`meta` is a JSON-serialisable dict, such as a display name.
None leaves the stream untracked.

The default, `django_pg_sse.presence.user_identity`, tracks logged-in users:

```python
("user:7", {"name": "ada"})
```

A site without accounts can name visitors by their session.
Every reader of the topic receives the identity, so it must not contain the session key itself:

```python
from django_pg_sse.messages import digest


def name_from_session(request):
    if "name" not in request.session:
        return None
    return f"session:{digest(request.session.session_key)}", {"name": request.session["name"]}
```

```python
PG_SSE = {
    "PRESENCE_TOPICS": ["room:"],
    "PRESENCE_IDENTITY": "myproject.events.name_from_session",
}
```

## Showing who is here

`present(topic)` returns the readers of a topic as `Presence` objects, in the order they joined.
It skips expired rows and rows closed past the grace period, even before a sweep deletes them.
`apresent(topic)` does the same in async code.

`presence_changed` is sent when a reader joins or leaves a topic.
Its arguments are `topic`, `joined` and `left`, two tuples of `Presence`, one of them empty.
A receiver renders the new state and broadcasts it to the topic:

```python
from django.dispatch import receiver
from django.template.loader import render_to_string
from django_pg_sse import abroadcast
from django_pg_sse.presence import apresent, presence_changed


@receiver(presence_changed)
async def show_who_is_here(sender, topic, joined, left, **kwargs):
    html = render_to_string("room.html#online", {"online": await apresent(topic)})
    await abroadcast(topic, html)
```

```html
{% partialdef online %}
<ul id="online" hx-swap-oob="true">
  {% for reader in online %}<li>{{ reader.meta.name }}</li>{% endfor %}
</ul>
{% endpartialdef %}
```

The signal is sent in the ASGI worker that saw the change, once per change across all workers.
Readers of the topic see presence through the same stream, so `CAN_SUBSCRIBE` also decides who sees it.

## Timing

A reader joins when its first stream on the topic opens.
A reader leaves 5 seconds after its last stream closes.
A reconnect within these 5 seconds sends neither a leave nor a join.

Each worker refreshes the rows of its open streams every 15 seconds.
A worker that is killed sends no close; its rows expire after 45 seconds without a refresh.
Any worker's next sweep then reports the leave.

[Settings](settings.md#fixed-values) lists these values.

## Storage

The table is UNLOGGED: writes skip the WAL, and standbys do not replicate it.
A crash empties it.
The next refresh of each worker inserts the rows of its open streams again and reports their readers as joined.
A worker whose rows another worker swept, such as after a pause of more than 45 seconds, gets them back the same way.

Presence queries run on the alias in `PG_SSE["DATABASE"]` and bypass database routers.
A router could otherwise send a read to a standby, which refuses UNLOGGED tables.
