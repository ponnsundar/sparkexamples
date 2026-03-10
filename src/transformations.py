"""Reusable Spark transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_nulls(df: DataFrame, subset: list[str] | None = None) -> DataFrame:
    """Drop rows with null values in specified columns."""
    return df.dropna(subset=subset)


def add_processing_timestamp(df: DataFrame) -> DataFrame:
    """Add a column with the current processing timestamp."""
    return df.withColumn("processed_at", F.current_timestamp())


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Lowercase and replace spaces with underscores in column names."""
    for col_name in df.columns:
        new_name = col_name.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(col_name, new_name)
    return df


def deduplicate(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep the latest row per key based on order_col."""
    from pyspark.sql.window import Window

    window = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
