# Deployment

## Server

Streams need an ASGI server, such as granian, uvicorn or daphne.
`manage.py runserver` speaks WSGI and cannot stream an async response.
Each ASGI worker runs one event loop.
Each event loop runs one hub, which listens for events.

## Database connections

[Database connections](index.md#database-connections) lists what a worker holds and which `OPTIONS` apply.
Add up to 2 + `PG_SSE["STREAM_CONNECTIONS"]` per worker to Django's own connections when you size `max_connections`.
[How it works](index.md#how-it-works) describes how the hub notices a lost connection.

## Reverse proxies

Once connected, a stream sends a comment line every 15 seconds, so proxy idle timeouts do not cut it.
While the hub reconnects, a newly opened stream sends nothing until the hub listens again.

Caddy 2's `reverse_proxy` flushes `text/event-stream` responses at once, whatever `flush_interval` says.
Its `encode` directive streams such responses as they are written, according to the Caddy docs.
nginx buffers proxied responses by default.
The stream sends `X-Accel-Buffering: no`, so nginx passes it through without `proxy_buffering off;`.

## Pruning

Every event is stored, so a reconnecting browser can replay what it missed.
Run `manage.py prune_stream_events` regularly, for example hourly from cron or a task scheduler.
It deletes events older than `PG_SSE["RETENTION_HOURS"]` (default 24).
