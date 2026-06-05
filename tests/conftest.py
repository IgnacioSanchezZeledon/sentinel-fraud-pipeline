"""Shared pytest fixtures for the test suite.

Provides a single session-scoped local Spark session so the transformation
unit tests run without Kafka, MinIO, or the Docker cluster.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Boot a quiet `local[1]` Spark session shared across the test session."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
