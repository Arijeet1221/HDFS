# Architecture

The existing HDFS data path is unchanged:

`Client -> NameNode socket -> DataNode sockets -> storage`

Kafka is an auxiliary event path:

`NameNode/DataNode -> JSON event producer -> Kafka topic -> consumers/dashboard`

The NameNode metadata JSON remains authoritative. Kafka events are not used to reconstruct placement or storage. File and chunk contents are never published to Kafka.

## Producers

- DataNodes publish `datanode.started`, `heartbeat.received`, and `chunk.created`.
- NameNode publishes `datanode.registered`, `datanode.failed`, `chunk.created`, `file.deleted`, `file.delete_pending`, and replication lifecycle events.

## Failure behavior

The producer is fail-open. Connection errors are logged with backoff, HDFS socket requests continue, and metadata remains authoritative. The dashboard consumer keeps only a bounded in-memory event history and ignores duplicate `event_id` values.
