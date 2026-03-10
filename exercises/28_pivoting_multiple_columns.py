# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Pivoting on Multiple Columns
# MAGIC Pivot on multiple columns by combining them into a single pivot key.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("Alice", "Sales", "Q1", 1000),
    ("Alice", "Sales", "Q2", 1200),
    ("Bob", "Engineering", "Q1", 2000),
    ("Bob", "Engineering", "Q2", 2200),
]
df = spark.createDataFrame(data, ["name", "department", "quarter", "revenue"])
df.show()

# COMMAND ----------

# Combine department + quarter as pivot key
solution = (
    df.withColumn("pivot_key", F.concat_ws("_", "department", "quarter"))
    .groupBy("name")
    .pivot("pivot_key")
    .agg(F.first("revenue"))
)
solution.show()
