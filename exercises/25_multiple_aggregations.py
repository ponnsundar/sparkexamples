# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Multiple Aggregations
# MAGIC Perform multiple aggregation functions in a single groupBy.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("Sales", "Q1", 1000),
    ("Sales", "Q2", 1500),
    ("Sales", "Q3", 1200),
    ("Sales", "Q4", 1800),
    ("Engineering", "Q1", 2000),
    ("Engineering", "Q2", 2200),
    ("Engineering", "Q3", 2100),
    ("Engineering", "Q4", 2500),
]
df = spark.createDataFrame(data, ["department", "quarter", "revenue"])
df.show()

# COMMAND ----------

solution = df.groupBy("department").agg(
    F.count("*").alias("num_quarters"),
    F.sum("revenue").alias("total_revenue"),
    F.avg("revenue").alias("avg_revenue"),
    F.min("revenue").alias("min_revenue"),
    F.max("revenue").alias("max_revenue"),
    F.stddev("revenue").alias("stddev_revenue"),
)
solution.show()
