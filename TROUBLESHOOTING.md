# Troubleshooting

## Kafka is unavailable

HDFS operations should continue. Check the service log for `[Kafka Producer] Kafka unavailable`. Start Kafka with `cd kafka; docker compose up -d; .\create-topics.ps1`.

## DN3 cannot publish events

The broker address must be reachable from EC2. On Linux EC2, run `export KAFKA_BOOTSTRAP_SERVERS=100.104.93.92:9092` before starting DN3, and restart the DN3 process after changing it. The Windows firewall must allow TCP `9092` on the Tailscale interface. This does not affect DN3 socket storage operations.

## No dashboard events

Check `GET http://127.0.0.1:5000/api/events`. An empty list means no events have been consumed by this client instance or Kafka is unavailable. Verify topics with `docker exec mini-hdfs-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092`.

## Duplicate events

Consumers use `event_id` for in-process duplicate suppression. The NameNode remains authoritative, so duplicate event delivery does not duplicate chunk data or metadata.
