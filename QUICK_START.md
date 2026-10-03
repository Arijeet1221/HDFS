# Quick Start

```powershell
cd 'C:\PESU\Big Data\HDFS'
.venv\Scripts\activate
cd kafka
docker compose up -d
$env:KAFKA_ADVERTISED_HOST = "100.104.93.92"
.\create-topics.ps1
cd ..
python namenode\namenode.py
python datanode1\datanode1.py
python datanode2\datanode2.py
python client\client.py
```

Open `http://127.0.0.1:5000`.

Kafka is an optional event layer. HDFS uploads, downloads, replication, and deletion continue if Kafka is stopped; events are logged and dropped until the broker returns.

For EC2 DataNode 3, install `kafka-python==3.0.11`, copy the updated project files, and start it with `KAFKA_BOOTSTRAP_SERVERS=100.104.93.92:9092`. The Windows firewall must allow TCP `9092` on the Tailscale interface.
