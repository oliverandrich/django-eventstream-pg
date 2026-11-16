# Changelog

## 0.1.0 (2026-11-16)

### 🚀 Features
- **`broadcast(topic, data, event="")`** sends HTML, or any text in a named event, to the open server-sent event streams of a topic after the transaction commits.
- **`broadcast_json(topic, value, event=...)`** sends JSON in a named event for client code.
- **`abroadcast()`, `abroadcast_json()` and `aclose_streams()`** do the same from async code.
- **One stream per page** for several topics, with `Last-Event-ID` replay from stored events.
  Event ids are signed for the stream's user, session and topics, so a client cannot pick its own replay start.
- **`close_streams(user=..., session=...)`** ends streams after access is taken away; logging out closes the streams of that session.
- **`stream_opened` and `stream_closed` signals** report the lifecycle of each stream, such as for presence or metrics.
- **System checks** for PostgreSQL, the `PG_SSE` values and the authentication middleware.
- **`PG_SSE["DATABASE"]`** gives the hub its own alias, such as one that bypasses a PgBouncer in transaction mode.
- **Timeouts** for the hub's queries (`QUERY_TIMEOUT`, 30 s) and connects (10 s), and `tcp_user_timeout` on Linux, so a failover holds a worker for a bounded time.
- **Keepalives** after 15 quiet seconds, also while a stream opens, and the `X-Accel-Buffering: no` header for nginx.
- **Reserved topics**: the stream view and `broadcast()` refuse topics starting with `_`.
- **`prune_stream_events`** deletes events older than the retention.
- **Documentation** on Read the Docs, with settings, a deployment guide and an API reference.
