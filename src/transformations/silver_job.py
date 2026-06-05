"""Silver job skeleton for Phase 3 micro-step 3.1.

Batch PySpark job that reads the Bronze Delta table, parses the raw JSON
`value` payload into the expected Silver column layout, and writes the
result to a Delta table at `s3a://silver/transactions/`. No transformation
logic (type casts, deduplication, feature engineering) is applied yet —
those land in micro-steps 3.2–3.5. The goal here is only to wire up the
read/write plumbing and pin down the target schema.
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType

logger = logging.getLogger(__name__)

DEFAULT_BRONZE_INPUT_PATH = "s3a://bronze/transactions/"
DEFAULT_SILVER_OUTPUT_PATH = "s3a://silver/transactions/"

PCA_FEATURE_COLUMNS: tuple[str, ...] = tuple(f"V{i}" for i in range(1, 29))
SOURCE_PAYLOAD_COLUMNS: tuple[str, ...] = (
    "Time",
    *PCA_FEATURE_COLUMNS,
    "Amount",
    "Class",
    "event_id",
    "event_timestamp",
)
AUDIT_COLUMNS: tuple[str, ...] = (
    "_kafka_partition",
    "_kafka_offset",
    "_ingested_at",
)


def build_payload_schema() -> StructType:
    """Schema used to parse the Bronze `value` JSON string.

    All fields are kept as StringType at this stage; type casting to the
    final Silver types happens in micro-step 3.2.
    """
    return StructType(
        [StructField(name, StringType(), nullable=True) for name in SOURCE_PAYLOAD_COLUMNS]
    )


def read_bronze(spark: SparkSession, input_path: str) -> DataFrame:
    """Load the Bronze Delta table as a batch DataFrame."""
    return spark.read.format("delta").load(input_path)


def to_silver(bronze: DataFrame) -> DataFrame:
    """Parse the JSON payload and project the expected Silver schema.

    No casts, dedup, or feature engineering yet — this is the noop
    skeleton for micro-step 3.1.
    """
    payload_schema = build_payload_schema()
    parsed = bronze.withColumn("_payload", from_json(col("value"), payload_schema))
    projected_payload = [col(f"_payload.{name}").alias(name) for name in SOURCE_PAYLOAD_COLUMNS]
    audit = [col(name) for name in AUDIT_COLUMNS]
    return parsed.select(*projected_payload, *audit)


def write_silver(silver: DataFrame, output_path: str) -> None:
    """Overwrite the Silver Delta table at `output_path`."""
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    input_path = os.getenv("BRONZE_OUTPUT_PATH", DEFAULT_BRONZE_INPUT_PATH)
    output_path = os.getenv("SILVER_OUTPUT_PATH", DEFAULT_SILVER_OUTPUT_PATH)

    logger.info("starting silver job: input=%s output=%s", input_path, output_path)

    spark = SparkSession.builder.appName("silver_job").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bronze = read_bronze(spark, input_path)
    silver = to_silver(bronze)
    write_silver(silver, output_path)

    logger.info("silver job finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
