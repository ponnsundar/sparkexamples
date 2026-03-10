# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Converting Arrays of Strings to String
# MAGIC Convert array columns to concatenated strings using array_join or concat_ws.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    (1, ["hello", "world"]),
    (2, ["apache", "spark", "databricks"]),
    (3, ["foo"]),
]
df = spark.createDataFrame(data, ["id", "words"])
df.show(truncate=False)

# COMMAND ----------

# Using concat_ws
solution1 = df.withColumn("words_str", F.concat_ws(", ", "words"))
solution1.show(truncate=False)

# COMMAND ----------

# Using array_join (Spark 2.4+)
solution2 = df.withColumn("words_str", F.array_join("words", ", "))
solution2.show(truncate=False)
