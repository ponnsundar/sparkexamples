# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Flattening Array Columns
# MAGIC Convert datasets of arrays into datasets of individual array elements using explode.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    (1, ["a", "b", "c"]),
    (2, ["d", "e"]),
    (3, ["f"]),
]
df = spark.createDataFrame(data, ["id", "values"])
df.show(truncate=False)

# COMMAND ----------

# Flatten using explode
solution = df.select("id", F.explode("values").alias("value"))
solution.show()

# COMMAND ----------

# Use explode_outer to keep rows with empty/null arrays
data_with_empty = data + [(4, [])]
df2 = spark.createDataFrame(data_with_empty, ["id", "values"])
solution2 = df2.select("id", F.explode_outer("values").alias("value"))
solution2.show()
