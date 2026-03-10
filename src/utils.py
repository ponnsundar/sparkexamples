"""Utility helpers for Spark / Databricks."""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "spark_databricks_project") -> SparkSession:
    """Return or create a SparkSession with Delta Lake support."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_delta(spark: SparkSession, path: str):
    """Read a Delta table."""
    return spark.read.format("delta").load(path)


def write_delta(df, path: str, mode: str = "overwrite", partition_by: list[str] | None = None):
    """Write a DataFrame as a Delta table."""
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
