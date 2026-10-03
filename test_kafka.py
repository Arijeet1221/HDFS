"""Offline Kafka integration checks; no broker is required."""

from common.events import make_event, topic_for_event
from common.kafka_producer import publish_event


def main():
    event = make_event("chunk.created", "DN-5002", {"chunk_id": "demo", "size": 1})
    assert event["event_id"]
    assert event["event_type"] == "chunk.created"
    assert topic_for_event("heartbeat.received") == "heartbeat-events"
    assert topic_for_event("replication.required") == "replication-events"
    assert "data" not in event["payload"]
    result = publish_event("system.startup", "test", {"offline_test": True})
    assert result["event_type"] == "system.startup"
    print("Kafka offline/schema checks: PASS")


if __name__ == "__main__":
    main()
