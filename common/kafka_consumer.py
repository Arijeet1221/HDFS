"""Optional Kafka consumer for dashboard and replication event views."""

import json
import logging
import os
import sys
import threading
import time

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONNECT_TIMEOUT_MS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_ENABLED,
    KAFKA_RETRY_BACKOFF_SECONDS,
    KAFKA_TOPICS,
)

try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    original_sys_path = sys.path[:]
    sys.path = [
        path for path in sys.path
        if os.path.abspath(path or os.curdir) != project_root
    ]
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None
finally:
    if "original_sys_path" in locals():
        sys.path = original_sys_path

logger = logging.getLogger("mini_hdfs.kafka")


class KafkaEventConsumer:
    def __init__(self, topics=None, group_id=KAFKA_CONSUMER_GROUP):
        self.topics = topics or list(KAFKA_TOPICS)
        self.group_id = group_id
        self._consumer = None
        self._stop = threading.Event()

    def consume_forever(self, on_event):
        if not KAFKA_ENABLED or KafkaConsumer is None:
            return
        seen = set()
        while not self._stop.is_set():
            try:
                self._consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=self.group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                    request_timeout_ms=KAFKA_CONNECT_TIMEOUT_MS,
                    value_deserializer=lambda value: json.loads(
                        value.decode("utf-8")
                    ),
                )
                for message in self._consumer:
                    if self._stop.is_set():
                        break
                    event = message.value
                    event_id = event.get("event_id")
                    if event_id and event_id in seen:
                        continue
                    if event_id:
                        seen.add(event_id)
                        if len(seen) > 1000:
                            seen.clear()
                    on_event(event)
            except Exception as exc:
                logger.warning(
                    "[Kafka Consumer] Kafka unavailable; retrying: %s",
                    exc,
                )
            finally:
                if self._consumer is not None:
                    self._consumer.close()
                    self._consumer = None

            if not self._stop.is_set():
                time.sleep(KAFKA_RETRY_BACKOFF_SECONDS)

    def close(self):
        self._stop.set()
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
