"""Kafka event schema for Mini-HDFS metadata events."""

from datetime import datetime, timezone
import uuid


def make_event(event_type, node_id=None, payload=None):
    """Create a JSON-safe event envelope without file or chunk bytes."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "node_id": node_id,
        "payload": payload or {},
    }


EVENT_TOPIC_BY_PREFIX = {
    "datanode.": "datanode-events",
    "heartbeat.": "heartbeat-events",
    "chunk.": "chunk-events",
    "replication.": "replication-events",
    "system.": "system-events",
}


def topic_for_event(event_type):
    for prefix, topic in EVENT_TOPIC_BY_PREFIX.items():
        if event_type.startswith(prefix):
            return topic
    return "system-events"
