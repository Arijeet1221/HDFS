$topics = @(
  'datanode-events',
  'heartbeat-events',
  'chunk-events',
  'replication-events',
  'system-events'
)
foreach ($topic in $topics) {
  docker exec mini-hdfs-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic $topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
}
