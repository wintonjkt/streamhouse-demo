"""Kafka helpers: topic management, publishing with delivery reports, and tailing a topic."""
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from . import config
from .config import settings

# name -> (partitions, config)
TOPICS: Dict[str, Tuple[int, Dict[str, str]]] = {
    config.TOPIC_TRANSACTIONS: (3, {}),
    config.TOPIC_DIGITAL: (3, {}),
    config.TOPIC_STATE: (3, {}),
    config.TOPIC_ALERTS: (3, {}),
    # Reference data mirrored from Db2: compacted, one record per key
    config.TOPIC_CUSTOMER: (1, {"cleanup.policy": "compact"}),
    config.TOPIC_ACCOUNTS: (1, {"cleanup.policy": "compact"}),
}

_producer: Optional[Producer] = None


class KafkaError_(RuntimeError):
    pass


def _admin() -> AdminClient:
    return AdminClient(settings.kafka_client_config)


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        # Keepalive and short timeouts: idle broker connections are dropped on this network, and a
        # stuck produce request must fail fast instead of hanging the API.
        _producer = Producer(dict(settings.kafka_client_config, **{
            "acks": "all",
            "enable.idempotence": False,
            "socket.keepalive.enable": True,
            "request.timeout.ms": 10000,
            "message.timeout.ms": 15000,
            "retries": 3,
        }))
    return _producer


def _reset_producer() -> None:
    global _producer
    _producer = None


def list_topics(timeout: float = 15.0) -> List[str]:
    try:
        return sorted(_admin().list_topics(timeout=timeout).topics)
    except KafkaException as exc:
        raise KafkaError_("Kafka metadata request failed: %s" % exc)


def ensure_topics() -> Dict[str, Any]:
    admin = _admin()
    existing = set(list_topics())
    wanted = [
        NewTopic(name, num_partitions=parts, replication_factor=3, config=cfg)
        for name, (parts, cfg) in TOPICS.items()
        if name not in existing
    ]
    result = {"already_existed": sorted(set(TOPICS) & existing), "created": [], "failed": {}}
    if wanted:
        for name, future in admin.create_topics(wanted, operation_timeout=30).items():
            try:
                future.result()
                result["created"].append(name)
            except Exception as exc:
                result["failed"][name] = str(exc)
    return result


def publish(topic: str, key: str, value: Dict[str, Any], timeout: float = 20.0) -> Dict[str, Any]:
    """Publish a JSON record and wait for the broker acknowledgement (one retry on a fresh producer)."""
    last_error = "delivery timeout"
    for _ in range(2):
        outcome: Dict[str, Any] = {}

        def on_delivery(err, msg, outcome=outcome):
            if err is not None:
                outcome["error"] = str(err)
            else:
                outcome.update(partition=msg.partition(), offset=msg.offset())

        producer = _get_producer()
        producer.produce(topic, key=key, value=json.dumps(value, default=str).encode("utf-8"),
                         on_delivery=on_delivery)
        remaining = producer.flush(timeout)
        if not remaining and "error" not in outcome:
            return {"topic": topic, "partition": outcome["partition"], "offset": outcome["offset"]}
        last_error = outcome.get("error", "delivery timeout")
        _reset_producer()
    raise KafkaError_("Publish to %s failed: %s" % (topic, last_error))


def tail(topic: str, limit: int = 20, timeout: float = 8.0) -> List[Dict[str, Any]]:
    """Return up to `limit` most recent records per partition, newest last."""
    consumer = Consumer(dict(settings.kafka_client_config, **{
        "group.id": "viewer-" + uuid.uuid4().hex[:8],
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }))
    try:
        meta = consumer.list_topics(topic, timeout=timeout)
        if topic not in meta.topics or meta.topics[topic].error is not None:
            raise KafkaError_("Topic %s does not exist" % topic)
        assignments, ends = [], {}
        for pid in meta.topics[topic].partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=timeout)
            if high > low:
                assignments.append(TopicPartition(topic, pid, max(low, high - limit)))
                ends[pid] = high
        if not assignments:
            return []
        consumer.assign(assignments)
        records, done = [], set()
        deadline = time.time() + timeout
        while len(done) < len(assignments) and time.time() < deadline:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            raw = msg.value()
            try:
                value = json.loads(raw) if raw else None
            except ValueError:
                value = raw.decode("utf-8", "replace") if raw else None
            records.append({
                "partition": msg.partition(), "offset": msg.offset(),
                "timestamp": msg.timestamp()[1],
                "key": msg.key().decode("utf-8", "replace") if msg.key() else None,
                "value": value,
            })
            if msg.offset() >= ends[msg.partition()] - 1:
                done.add(msg.partition())
        records.sort(key=lambda r: (r["timestamp"], r["partition"], r["offset"]))
        return records[-limit:]
    except KafkaException as exc:
        raise KafkaError_("Kafka read failed: %s" % exc)
    finally:
        consumer.close()
