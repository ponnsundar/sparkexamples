"""Unit tests for transformations — run locally with pytest."""

import pytest
from pyspark.sql import SparkSession

from src.transformations import clean_nulls, normalize_column_names, deduplicate


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("tests")
        .getOrCreate()
    )


def test_clean_nulls(spark):
    df = spark.createDataFrame([{"a": 1, "b": None}, {"a": 2, "b": "x"}])
    result = clean_nulls(df, subset=["b"])
    assert result.count() == 1


def test_normalize_column_names(spark):
    df = spark.createDataFrame([{"First Name": "A", "Last Name": "B"}])
    result = normalize_column_names(df)
    assert set(result.columns) == {"first_name", "last_name"}


def test_deduplicate(spark):
    data = [
        {"id": 1, "value": "old", "ts": 1},
        {"id": 1, "value": "new", "ts": 2},
        {"id": 2, "value": "only", "ts": 1},
    ]
    df = spark.createDataFrame(data)
    result = deduplicate(df, keys=["id"], order_col="ts")
    assert result.count() == 2
    row = result.filter(result.id == 1).collect()[0]
    assert row["value"] == "new"
