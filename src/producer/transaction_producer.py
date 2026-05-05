"""Minimal Kafka producer for Phase 2 micro-step 2.2.

Sends a single hardcoded JSON message to verify Kafka connectivity from the
host. Real CSV-driven publishing arrives in micro-step 2.3.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "transactions"
DELIVERY_TIMEOUT_SECONDS = 10

TEST_MESSAGE: dict[str, object] = {
    "event_id": "00000000-0000-0000-0000-000000000000",
    "event_timestamp": "2026-05-05T00:00:00Z",
    "source": "transaction_producer --test",
    "payload": {"hello": "kafka"},
}


def build_producer(broker: str) -> KafkaProducer:
    """Create a KafkaProducer that serializes values as UTF-8 JSON."""
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def send_test_message(broker: str, topic: str) -> None:
    """Send a single hardcoded message and block until the broker acks it."""
    producer = build_producer(broker)
    try:
        future = producer.send(topic, TEST_MESSAGE)
        metadata = future.get(timeout=DELIVERY_TIMEOUT_SECONDS)
        logger.info(
            "delivered to topic=%s partition=%s offset=%s",
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )
    except KafkaError:
        logger.exception("failed to deliver test message")
        raise
    finally:
        producer.flush()
        producer.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Minimal Kafka producer (Phase 2 micro-step 2.2)."
    )
    parser.add_argument(
        "--test",
        action="store_true",
        required=True,
        help="Send a single hardcoded test message to the configured topic.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    parse_args(argv)
    broker = os.getenv("KAFKA_BROKER", DEFAULT_BROKER)
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    logger.info("connecting to broker=%s topic=%s", broker, topic)
    send_test_message(broker, topic)
    return 0


if __name__ == "__main__":
    sys.exit(main())
