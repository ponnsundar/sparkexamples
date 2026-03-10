# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Finding Longest Sequence (Window Aggregation)
# MAGIC Find the longest consecutive sequence of the same value per group.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    ("A", 1, "X"),
    ("A", 2, "X"),
    ("A", 3, "Y"),
    ("A", 4, "X"),
    ("A", 5, "X"),
    ("A", 6, "X"),
    ("B", 1, "Y"),
    ("B", 2, "Y"),
    ("B", 3, "X"),
]
df = spark.createDataFrame(data, ["group", "seq", "value"])
df.show()

# COMMAND ----------

# Identify sequence breaks using lag
w = Window.partitionBy("group").orderBy("seq")

df2 = df.withColumn("prev_value", F.lag("value").over(w))
df2 = df2.withColumn(
    "new_group",
    F.when(F.col("value") != F.coalesce(F.col("prev_value"), F.lit("")), 1).otherwise(0)
)

# Create a running group id
df3 = df2.withColumn("run_id", F.sum("new_group").over(w))

# Count per run, find the longest
run_lengths = df3.groupBy("group", "value", "run_id").agg(F.count("*").alias("run_length"))

w2 = Window.partitionBy("group").orderBy(F.col("run_length").desc())
solution = (
    run_lengths.withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("group", "value", "run_length")
)
solution.show()
