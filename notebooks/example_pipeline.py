# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Example Spark Pipeline
# MAGIC A simple ETL pipeline demonstrating project structure.

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from src.utils import get_spark, write_delta
from src.transformations import (
    clean_nulls,
    add_processing_timestamp,
    normalize_column_names,
)

# COMMAND ----------

spark = get_spark()

# COMMAND ----------

# Sample data — replace with your actual source
data = [
    {"Name": "Alice", "Age": 30, "City": "Seattle"},
    {"Name": "Bob", "Age": None, "City": "Portland"},
    {"Name": "Charlie", "Age": 25, "City": "Seattle"},
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

# Transform
df_clean = (
    df
    .transform(normalize_column_names)
    .transform(lambda d: clean_nulls(d, subset=["age"]))
    .transform(add_processing_timestamp)
)
df_clean.show(truncate=False)

# COMMAND ----------

# Write to Delta (update path for your environment)
# write_delta(df_clean, "/mnt/delta/example_output")

print("Pipeline complete.")
