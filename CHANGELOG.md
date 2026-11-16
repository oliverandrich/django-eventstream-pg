# Changelog

## Unreleased

### 🚀 Features
- **`broadcast(topic, data, event="")`** sends HTML, or any text in a named event, to the open server-sent event streams of a topic after the transaction commits.
- **`broadcast_json(topic, value, event=...)`** sends JSON in a named event for client code.
- **One stream per page** for several topics, with `Last-Event-ID` replay from stored events.
  Event ids are signed per user, session and topics, so a client cannot pick its own replay start.
- **`close_streams(user=..., session=...)`** ends streams after access is taken away; logging out closes the streams of that session.
- **`stream_opened` and `stream_closed` signals** report the lifecycle of each stream, such as for presence or metrics.
- **Reserved topics**: the stream view and `broadcast()` refuse topics starting with `_`.
- **`prune_stream_events`** deletes events older than the retention.
