"""CSV-driven Kafka producer for Phase 2 micro-step 2.3.

Reads transactions from `data/creditcard.csv`, enriches each row with
`event_id` (UUID4) and `event_timestamp` (ISO 8601 UTC), and publishes
to the configured Kafka topic. Currently supports a single mode
(`slow`, 1 msg/s); fast/realistic modes arrive in micro-step 2.4.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "transactions"
DEFAULT_CSV_PATH = "data/creditcard.csv"
DELIVERY_TIMEOUT_SECONDS = 10
SLOW_MODE_DELAY_SECONDS = 1.0


def build_producer(broker: str) -> KafkaProducer:
    """Create a KafkaProducer that serializes values as UTF-8 JSON."""
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def enrich_row(row: dict[str, str]) -> dict[str, object]:
    """Return a copy of `row` with `event_id` and `event_timestamp` added."""
    return {
        **row,
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(tz=UTC).isoformat(timespec="microseconds"),
    }


def iter_rows(csv_path: Path, limit: int) -> Iterator[dict[str, str]]:
    """Yield rows from `csv_path`. If `limit > 0`, stop after `limit` rows."""
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for index, row in enumerate(reader):
            if limit > 0 and index >= limit:
                return
            yield row


def publish_csv(
    producer: KafkaProducer,
    topic: str,
    csv_path: Path,
    limit: int,
    delay_seconds: float,
) -> int:
    """Publish enriched rows to `topic`. Returns the number of messages sent."""
    sent = 0
    for row in iter_rows(csv_path, limit):
        message = enrich_row(row)
        future = producer.send(topic, message)
        future.get(timeout=DELIVERY_TIMEOUT_SECONDS)
        sent += 1
        logger.debug("sent event_id=%s seq=%d", message["event_id"], sent)
        if delay_seconds > 0:
            time.sleep(delay_seconds)
    return sent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CSV-driven Kafka producer (Phase 2 micro-step 2.3)."
    )
    parser.add_argument(
        "--mode",
        choices=["slow"],
        required=True,
        help="Publishing rate mode. `slow` = 1 msg/s. (fast/realistic in 2.4)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Stop after N messages (0 = all rows in the CSV).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    args = parse_args(argv)
    broker = os.getenv("KAFKA_BROKER", DEFAULT_BROKER)
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    csv_path = Path(os.getenv("PRODUCER_CSV_PATH", DEFAULT_CSV_PATH))

    if not csv_path.is_file():
        logger.error("csv not found at %s", csv_path)
        return 2

    delay = SLOW_MODE_DELAY_SECONDS

    logger.info(
        "publishing from %s to broker=%s topic=%s mode=%s limit=%s",
        csv_path,
        broker,
        topic,
        args.mode,
        args.limit if args.limit > 0 else "all",
    )

    producer = build_producer(broker)
    try:
        sent = publish_csv(producer, topic, csv_path, args.limit, delay)
        logger.info("done. published %d messages.", sent)
    except KafkaError:
        logger.exception("failed during publish")
        return 1
    finally:
        producer.flush()
        producer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
