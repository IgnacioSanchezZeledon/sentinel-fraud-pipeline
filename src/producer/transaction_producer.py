"""CSV-driven Kafka producer for Phase 2 micro-step 2.4.

Reads transactions from `data/creditcard.csv`, enriches each row with
`event_id` (UUID4) and `event_timestamp` (ISO 8601 UTC), and publishes
to the configured Kafka topic. Supports three modes (`slow`, `fast`,
`realistic`), renders a tqdm progress bar, and shuts down gracefully on
SIGINT/SIGTERM by flushing pending messages before closing.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError
from tqdm import tqdm

logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "transactions"
DEFAULT_CSV_PATH = "data/creditcard.csv"
SLOW_MODE_DELAY_SECONDS = 1.0
REALISTIC_MEAN_DELAY_SECONDS = 0.2
REALISTIC_MAX_DELAY_SECONDS = 2.0


class ShutdownFlag:
    """Boolean flag flipped by the SIGINT/SIGTERM handler."""

    def __init__(self) -> None:
        self._requested = False

    @property
    def requested(self) -> bool:
        return self._requested

    def request(self) -> None:
        self._requested = True


def install_signal_handlers(flag: ShutdownFlag) -> None:
    """Install handlers that flip `flag` when SIGINT or SIGTERM arrives."""

    def handler(signum: int, _frame: object) -> None:
        logger.info("shutdown requested (signal %s); finishing current batch...", signum)
        flag.request()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


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


def make_delay_fn(mode: str) -> Callable[[], float]:
    """Return a callable that yields the next sleep duration for `mode`."""
    if mode == "slow":
        return lambda: SLOW_MODE_DELAY_SECONDS
    if mode == "fast":
        return lambda: 0.0
    if mode == "realistic":
        rate = 1.0 / REALISTIC_MEAN_DELAY_SECONDS
        return lambda: min(random.expovariate(rate), REALISTIC_MAX_DELAY_SECONDS)
    raise ValueError(f"unknown mode: {mode}")


def publish_csv(
    producer: KafkaProducer,
    topic: str,
    csv_path: Path,
    limit: int,
    delay_fn: Callable[[], float],
    shutdown: ShutdownFlag,
) -> int:
    """Publish enriched rows to `topic`. Returns the number of messages queued."""
    queued = 0
    total = limit if limit > 0 else None
    with tqdm(total=total, unit="msg", desc="publishing") as bar:
        for row in iter_rows(csv_path, limit):
            if shutdown.requested:
                break
            producer.send(topic, enrich_row(row))
            queued += 1
            bar.update(1)
            wait = delay_fn()
            if wait > 0:
                time.sleep(wait)
    return queued


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CSV-driven Kafka producer (Phase 2 micro-step 2.4)."
    )
    parser.add_argument(
        "--mode",
        choices=["slow", "fast", "realistic"],
        required=True,
        help="Publishing rate. slow=1msg/s, fast=no delay, realistic=Poisson arrivals.",
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

    shutdown = ShutdownFlag()
    install_signal_handlers(shutdown)

    logger.info(
        "publishing from %s to broker=%s topic=%s mode=%s limit=%s",
        csv_path,
        broker,
        topic,
        args.mode,
        args.limit if args.limit > 0 else "all",
    )

    producer = build_producer(broker)
    delay_fn = make_delay_fn(args.mode)
    queued = 0
    try:
        queued = publish_csv(producer, topic, csv_path, args.limit, delay_fn, shutdown)
        logger.info("queued %d messages; flushing...", queued)
    except KafkaError:
        logger.exception("failed during publish")
        return 1
    finally:
        producer.flush()
        producer.close()
    logger.info("done. delivered %d messages.", queued)
    return 0


if __name__ == "__main__":
    sys.exit(main())
