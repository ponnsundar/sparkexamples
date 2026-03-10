# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Finding IDs of Rows with Word in Array Column
# MAGIC Find rows where an array column contains a specific word.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    (1, ["hello", "world"]),
    (2, ["foo", "bar", "hello"]),
    (3, ["spark", "databricks"]),
    (4, ["hello", "spark"]),
]
df = spark.createDataFrame(data, ["id", "words"])
df.show(truncate=False)

# COMMAND ----------

# Find rows where the array contains "hello"
solution = df.filter(F.array_contains(F.col("words"), "hello"))
solution.show(truncate=False)
