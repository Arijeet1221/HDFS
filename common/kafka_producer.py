"""Fail-open Kafka producer used by HDFS services."""

import json
import logging
import os
import sys
import threading
import time

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONNECT_TIMEOUT_MS,
    KAFKA_ENABLED,
    KAFKA_RETRY_BACKOFF_SECONDS,
)
from common.events import make_event, topic_for_event

try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    original_sys_path = sys.path[:]
    sys.path = [
        path for path in sys.path
        if os.path.abspath(path or os.curdir) != project_root
    ]
    from kafka import KafkaProducer
except ImportError:  # Kafka is optional at runtime.
    KafkaProducer = None
finally:
    if "original_sys_path" in locals():
        sys.path = original_sys_path


logger = logging.getLogger("mini_hdfs.kafka")


class KafkaEventProducer:
    """Best-effort producer; Kafka outages never fail HDFS operations."""

    def __init__(self):
        self._producer = None
        self._lock = threading.Lock()
        self._last_failure = 0.0

    def _get_producer(self):
        if not KAFKA_ENABLED or KafkaProducer is None:
            return None
        with self._lock:
            if self._producer is None:
                self._producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    request_timeout_ms=KAFKA_CONNECT_TIMEOUT_MS,
                    max_block_ms=KAFKA_CONNECT_TIMEOUT_MS,
                    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                )
            return self._producer

    def publish(self, event_type, node_id=None, payload=None, topic=None):
        event = make_event(event_type, node_id, payload)
        if not KAFKA_ENABLED:
            return event
        try:
            producer = self._get_producer()
            if producer is None:
                return event
            future = producer.send(
                topic or topic_for_event(event_type),
                value=event,
            )
            future.get(timeout=KAFKA_CONNECT_TIMEOUT_MS / 1000)
            logger.info("[Kafka Producer] Published: %s", event_type)
        except Exception as exc:
            now = time.time()
            if now - self._last_failure >= KAFKA_RETRY_BACKOFF_SECONDS:
                logger.warning("[Kafka Producer] Kafka unavailable: %s", exc)
                self._last_failure = now
            with self._lock:
                self._producer = None
        return event


_default_producer = KafkaEventProducer()


def publish_event(event_type, node_id=None, payload=None, topic=None):
    return _default_producer.publish(event_type, node_id, payload, topic)
