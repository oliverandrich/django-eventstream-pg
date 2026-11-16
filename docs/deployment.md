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

A stream sends a comment line after 15 quiet seconds, so proxy idle timeouts do not cut it.
This holds whatever the stream waits for, such as a busy connection pool or a reconnecting hub.
A stream whose hub has not listened for 60 seconds ends, and the browser reconnects.

Caddy 2's `reverse_proxy` flushes `text/event-stream` responses at once, whatever `flush_interval` says.
Its `encode` directive streams such responses as they are written, according to the Caddy docs.
nginx buffers proxied responses by default.
The stream sends `X-Accel-Buffering: no`, so nginx passes it through without `proxy_buffering off;`.

## Pruning

Every event is stored, so a reconnecting browser can replay what it missed.
Run `manage.py prune_stream_events` regularly, for example hourly from cron or a task scheduler.
It deletes events older than `PG_SSE["RETENTION_HOURS"]` (default 24).
