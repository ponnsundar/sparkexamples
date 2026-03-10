# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Split Function with Variable Delimiter Per Row
# MAGIC Write a structured query that splits a column using delimiters from another column.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^"),
]
df = spark.createDataFrame(data, ["VALUES", "Delimiter"])
df.show(truncate=False)

# COMMAND ----------

# Split using the per-row delimiter
solution = df.withColumn(
    "split_values",
    F.split(F.col("VALUES"), F.col("Delimiter"))
)
solution.show(truncate=False)

# COMMAND ----------

# EXTRA: Remove empty tokens from the split result
extra = solution.withColumn(
    "extra",
    F.filter(F.col("split_values"), lambda x: x != "")
)
extra.show(truncate=False)
