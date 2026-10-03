# Mini-HDFS with Kafka Events

This project is an educational HDFS implementation. The NameNode remains the source of truth and existing socket communication carries file and chunk data. Kafka carries metadata-only events for observability and asynchronous integration.

## Architecture

- NameNode: `127.0.0.1:8000`
- DataNode 1: `127.0.0.1:8001`
- DataNode 2: `127.0.0.1:8002`
- DataNode 3: `100.114.20.84:8003`
- Flask dashboard: `127.0.0.1:5000`
- Kafka KRaft broker: `127.0.0.1:9092`

Kafka never carries file contents or chunk bytes.

## Start Kafka

```powershell
cd kafka
$env:KAFKA_ADVERTISED_HOST = "100.104.93.92"
docker compose up -d
.\create-topics.ps1
```

Kafka is optional at runtime. Set `KAFKA_ENABLED=false` to run HDFS without a broker. For EC2 DN3, set `KAFKA_BOOTSTRAP_SERVERS=100.104.93.92:9092` before starting DN3.

## Start HDFS

From the repository root, start the existing services in this order:

```powershell
.venv\Scripts\activate
python namenode\namenode.py
python datanode1\datanode1.py
python datanode2\datanode2.py
python datanode3\datanode3.py
python client\client.py
```

DataNode 3 may run on EC2 instead of the local machine. Set `KAFKA_BOOTSTRAP_SERVERS` to a broker address reachable by that host if it should publish events.

## Event topics

`datanode-events`, `heartbeat-events`, `chunk-events`, `replication-events`, and `system-events` contain JSON envelopes with `event_id`, `event_type`, `timestamp`, `node_id`, and `payload`.

Inspect events:

```powershell
docker exec mini-hdfs-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic system-events --from-beginning
```

The dashboard exposes the received event read model at `GET /api/events`.
