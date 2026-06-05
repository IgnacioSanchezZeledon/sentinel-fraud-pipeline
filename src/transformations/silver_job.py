"""Silver job for Phase 3.

Batch PySpark job that reads the Bronze Delta table, parses the raw JSON
`value` payload, applies type casts (V1–V28 → Double, Amount → Decimal,
Class → Integer), drops rows where any required typed column is null,
deduplicates by `event_id`, derives temporal/amount/window features,
and writes the result as Delta at `s3a://silver/transactions/`.
"""

from __future__ import annotations

import logging
import math
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    cos,
    dayofweek,
    from_json,
    hour,
    lit,
    mean,
    sin,
    stddev,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

AMOUNT_DECIMAL_PRECISION = 18
AMOUNT_DECIMAL_SCALE = 2

HOURS_PER_DAY = 24

AMOUNT_BIN_LOW_MAX = 10
AMOUNT_BIN_MEDIUM_MAX = 100
AMOUNT_BIN_HIGH_MAX = 1000

WINDOW_SIZE_LAST_N = 5
HIGH_AMOUNT_ZSCORE_THRESHOLD = 2.0

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
    """Parse the JSON payload and project the expected Silver layout.

    Source payload fields come out as StringType; casts and downstream
    cleaning are applied by separate steps in `main`.
    """
    payload_schema = build_payload_schema()
    parsed = bronze.withColumn("_payload", from_json(col("value"), payload_schema))
    projected_payload = [col(f"_payload.{name}").alias(name) for name in SOURCE_PAYLOAD_COLUMNS]
    audit = [col(name) for name in AUDIT_COLUMNS]
    return parsed.select(*projected_payload, *audit)


def apply_casts(df: DataFrame) -> DataFrame:
    """Cast PCA features to Double, Amount to Decimal, and Class to Integer.

    `Time` and `event_timestamp` stay as StringType at this stage; the
    timestamp parse happens later when temporal features are derived.
    """
    casted = df
    for column_name in PCA_FEATURE_COLUMNS:
        casted = casted.withColumn(column_name, col(column_name).cast(DoubleType()))
    casted = casted.withColumn(
        "Amount",
        col("Amount").cast(DecimalType(AMOUNT_DECIMAL_PRECISION, AMOUNT_DECIMAL_SCALE)),
    )
    casted = casted.withColumn("Class", col("Class").cast(IntegerType()))
    return casted


def drop_invalid_rows(df: DataFrame) -> DataFrame:
    """Drop rows where any required typed column is null.

    A null in a typed column means either the source was already null or
    the cast in `apply_casts` could not parse the string — both are
    treated as unusable rows for downstream modeling.
    """
    required_columns = [*PCA_FEATURE_COLUMNS, "Amount", "Class"]
    return df.dropna(subset=required_columns)


def deduplicate(df: DataFrame) -> DataFrame:
    """Drop duplicate rows by `event_id`.

    Bronze can receive duplicates from Kafka at-least-once retries; this
    keeps Silver one row per event. The winning row is arbitrary, which
    is acceptable because rows sharing an `event_id` carry the same
    business payload.
    """
    return df.dropDuplicates(["event_id"])


def add_temporal_features(df: DataFrame) -> DataFrame:
    """Derive hour-of-day, day-of-week, and cyclic hour encoding.

    `transaction_day_of_week` follows Spark's `dayofweek` convention
    (1 = Sunday, 7 = Saturday). The sin/cos encoding maps the discrete
    hour onto the unit circle so that hour 23 sits next to hour 0,
    avoiding the false ordinal gap a raw integer would imply.
    """
    parsed_ts = to_timestamp(col("event_timestamp"))
    radians_per_hour = 2 * math.pi / HOURS_PER_DAY
    return (
        df.withColumn("transaction_hour", hour(parsed_ts))
        .withColumn("transaction_day_of_week", dayofweek(parsed_ts))
        .withColumn("hour_sin", sin(col("transaction_hour") * radians_per_hour))
        .withColumn("hour_cos", cos(col("transaction_hour") * radians_per_hour))
    )


def add_amount_features(df: DataFrame) -> DataFrame:
    """Discretize `Amount` into a categorical bin.

    Bins (in dollars): low < 10 ≤ medium < 100 ≤ high < 1000 ≤ very_high.
    `Amount` is non-null at this stage because `drop_invalid_rows` ran
    earlier in the pipeline.
    """
    return df.withColumn(
        "amount_bin",
        when(col("Amount") < AMOUNT_BIN_LOW_MAX, "low")
        .when(col("Amount") < AMOUNT_BIN_MEDIUM_MAX, "medium")
        .when(col("Amount") < AMOUNT_BIN_HIGH_MAX, "high")
        .otherwise("very_high"),
    )


def add_window_features(df: DataFrame) -> DataFrame:
    """Derive rolling average, global z-score, and high-amount flag.

    `avg_amount_last_5` is a trailing 5-row mean (current row included)
    over the global event_timestamp ordering. With no `customer_id` yet
    (that lands in phase 5) the only meaningful ordering is global, so
    Spark will emit a single-partition warning at this scale.

    `amount_zscore` uses the global mean and sample stddev computed in
    a single agg pass, broadcast as literals — avoids a second window
    scan and keeps the warning to one.

    `is_high_amount` flags rows more than `HIGH_AMOUNT_ZSCORE_THRESHOLD`
    standard deviations above the global mean.
    """
    amount_double = col("Amount").cast(DoubleType())
    rolling_window = Window.orderBy("event_timestamp").rowsBetween(
        -(WINDOW_SIZE_LAST_N - 1), 0
    )

    with_rolling = df.withColumn(
        "avg_amount_last_5", avg(amount_double).over(rolling_window)
    )

    stats = with_rolling.agg(
        mean(amount_double).alias("mu"),
        stddev(amount_double).alias("sigma"),
    ).first()
    mu = float(stats["mu"])
    sigma = float(stats["sigma"])

    return (
        with_rolling.withColumn(
            "amount_zscore", (amount_double - lit(mu)) / lit(sigma)
        ).withColumn(
            "is_high_amount", col("amount_zscore") > HIGH_AMOUNT_ZSCORE_THRESHOLD
        )
    )


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
    silver = apply_casts(silver)
    silver = drop_invalid_rows(silver)
    silver = deduplicate(silver)
    silver = add_temporal_features(silver)
    silver = add_amount_features(silver)
    silver = add_window_features(silver)
    write_silver(silver, output_path)

    logger.info("silver job finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
