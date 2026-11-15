# Changelog

## Unreleased

### 🚀 Features
- **`broadcast(topic, html)`** sends HTML to the open server-sent event streams of a topic after the transaction commits.
- **One stream per page** for several topics, with `Last-Event-ID` replay from stored events.
- **`close_streams(user=..., session=...)`** ends streams after access is taken away; logging out closes the streams of that session.
- **`prune_stream_events`** deletes events older than the retention.
