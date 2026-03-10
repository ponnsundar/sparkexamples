# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Using explode Standard Function
# MAGIC Use `explode` to convert arrays of strings into individual rows.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    (1, ["apple", "banana", "cherry"]),
    (2, ["date", "elderberry"]),
    (3, ["fig"]),
]
df = spark.createDataFrame(data, ["id", "fruits"])
df.show(truncate=False)

# COMMAND ----------

solution = df.select("id", F.explode("fruits").alias("fruit"))
solution.show()

# COMMAND ----------

# With position using posexplode
solution_pos = df.select("id", F.posexplode("fruits").alias("pos", "fruit"))
solution_pos.show()
