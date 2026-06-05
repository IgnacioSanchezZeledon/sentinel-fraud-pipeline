"""Unit tests for the pure Silver transformation functions.

These exercise the `DataFrame -> DataFrame` helpers in isolation, building
in-memory typed DataFrames at the post-cast Silver stage. No Kafka, no
MinIO, no Delta — just the local Spark fixture from `conftest.py`.
"""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.transformations.silver_job import (
    AMOUNT_DECIMAL_PRECISION,
    AMOUNT_DECIMAL_SCALE,
    PCA_FEATURE_COLUMNS,
    add_amount_features,
    add_temporal_features,
    add_window_features,
    deduplicate,
    drop_invalid_rows,
)

AMOUNT_TYPE = DecimalType(AMOUNT_DECIMAL_PRECISION, AMOUNT_DECIMAL_SCALE)
FEATURE_COLUMNS = {
    "transaction_hour",
    "transaction_day_of_week",
    "hour_sin",
    "hour_cos",
    "amount_bin",
    "avg_amount_last_5",
    "amount_zscore",
    "is_high_amount",
}
AMOUNT_BINS = {"low", "medium", "high", "very_high"}


def _silver_schema() -> StructType:
    """Build the typed schema of a row at the post-cast Silver stage."""
    fields = [StructField(name, DoubleType(), True) for name in PCA_FEATURE_COLUMNS]
    fields += [
        StructField("Amount", AMOUNT_TYPE, True),
        StructField("Class", IntegerType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
    ]
    return StructType(fields)


def _row(
    event_id: str,
    amount: str | None,
    *,
    event_timestamp: str = "2024-01-01T08:30:00",
    label: int = 0,
    fill: float = 0.0,
    overrides: dict[str, float | None] | None = None,
) -> dict[str, object]:
    """Build one typed Silver row, defaulting every PCA feature to `fill`."""
    data: dict[str, object] = dict.fromkeys(PCA_FEATURE_COLUMNS, fill)
    data["Amount"] = None if amount is None else Decimal(amount)
    data["Class"] = label
    data["event_id"] = event_id
    data["event_timestamp"] = event_timestamp
    if overrides:
        data.update(overrides)
    return data


def _df(spark: SparkSession, rows: list[dict[str, object]]) -> DataFrame:
    """Materialize rows into a DataFrame matching the typed Silver schema."""
    schema = _silver_schema()
    ordered = [[row[field.name] for field in schema.fields] for row in rows]
    return spark.createDataFrame(ordered, schema)


def test_deduplicate_collapses_by_event_id(spark: SparkSession) -> None:
    df = _df(
        spark,
        [_row("a", "10.00"), _row("a", "10.00"), _row("b", "20.00")],
    )

    result = deduplicate(df)

    assert result.count() == 2
    assert {r.event_id for r in result.select("event_id").collect()} == {"a", "b"}


def test_drop_invalid_rows_removes_any_null_required(spark: SparkSession) -> None:
    df = _df(
        spark,
        [
            _row("ok", "10.00"),
            _row("null_amount", None),
            _row("null_v1", "5.00", overrides={"V1": None}),
        ],
    )

    result = drop_invalid_rows(df)

    assert result.count() == 1
    assert result.first().event_id == "ok"


def test_amount_bin_boundaries(spark: SparkSession) -> None:
    expected = {
        "b_low_min": ("5.00", "low"),
        "b_low_max": ("9.99", "low"),
        "b_medium_min": ("10.00", "medium"),
        "b_medium_max": ("99.99", "medium"),
        "b_high_min": ("100.00", "high"),
        "b_high_max": ("999.99", "high"),
        "b_very_high_min": ("1000.00", "very_high"),
        "b_very_high": ("5000.00", "very_high"),
    }
    df = _df(spark, [_row(event_id, amount) for event_id, (amount, _) in expected.items()])

    result = add_amount_features(df)

    actual = {r.event_id: r.amount_bin for r in result.collect()}
    assert actual == {event_id: bin_ for event_id, (_, bin_) in expected.items()}


def test_feature_pipeline_columns_and_ranges(spark: SparkSession) -> None:
    df = _df(
        spark,
        [
            _row("t1", "10.00", event_timestamp="2024-01-01T00:15:00"),
            _row("t2", "250.00", event_timestamp="2024-01-01T23:45:00"),
            _row("t3", "5000.00", event_timestamp="2024-01-02T12:00:00"),
        ],
    )

    result = add_window_features(add_amount_features(add_temporal_features(df)))

    assert FEATURE_COLUMNS.issubset(set(result.columns))
    for row in result.collect():
        assert 0 <= row.transaction_hour <= 23
        assert 1 <= row.transaction_day_of_week <= 7
        assert -1.0 <= row.hour_sin <= 1.0
        assert -1.0 <= row.hour_cos <= 1.0
        assert row.amount_bin in AMOUNT_BINS
        assert row.avg_amount_last_5 is not None
        assert row.amount_zscore is not None
