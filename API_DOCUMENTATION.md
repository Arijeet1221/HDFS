# API Documentation

Existing HDFS APIs remain unchanged.

## Event feed

`GET /api/events`

Returns:

```json
{"status":"success","events":[{"event_id":"...","event_type":"heartbeat.received","timestamp":"...","node_id":"DN-5002","payload":{}}]}
```

## Kafka event envelope

Every event contains:

- `event_id`: UUID used for idempotency
- `event_type`: dotted event name
- `timestamp`: UTC ISO-8601 timestamp
- `node_id`: DataNode identifier when applicable
- `payload`: metadata only; never file or chunk bytes
