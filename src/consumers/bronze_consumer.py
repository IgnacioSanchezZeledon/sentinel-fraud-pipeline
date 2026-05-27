"""Bronze consumer for Phase 2 micro-steps 2.5–2.6.

PySpark Structured Streaming job that subscribes to the Kafka transactions
topic and appends raw records (Kafka envelope + JSON value as string) to a
Delta table at `s3a://bronze/transactions/`. The Spark Streaming checkpoint
provides exactly-once semantics: restarting the job resumes from the last
committed offsets without duplicating rows.
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

DEFAULT_KAFKA_BOOTSTRAP = "kafka:29092"
DEFAULT_KAFKA_TOPIC = "transactions"
DEFAULT_BRONZE_OUTPUT_PATH = "s3a://bronze/transactions/"
DEFAULT_BRONZE_CHECKPOINT_PATH = "s3a://bronze/checkpoints/transactions/"


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP)
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)
    output_path = os.getenv("BRONZE_OUTPUT_PATH", DEFAULT_BRONZE_OUTPUT_PATH)
    checkpoint_path = os.getenv("BRONZE_CHECKPOINT_PATH", DEFAULT_BRONZE_CHECKPOINT_PATH)

    logger.info(
        "starting bronze consumer: bootstrap=%s topic=%s output=%s checkpoint=%s",
        bootstrap,
        topic,
        output_path,
        checkpoint_path,
    )

    spark = SparkSession.builder.appName("bronze_consumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze = raw.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS value",
        "topic",
        "partition AS _kafka_partition",
        "offset AS _kafka_offset",
        "timestamp AS kafka_timestamp",
        "timestampType AS kafka_timestamp_type",
        "current_timestamp() AS _ingested_at",
    )

    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
    )

    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
