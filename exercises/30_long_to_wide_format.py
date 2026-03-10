# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Flattening Dataset from Long to Wide Format
# MAGIC Convert a long-format dataset to wide format using pivot.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    (1, "height", "180"),
    (1, "weight", "75"),
    (1, "age", "30"),
    (2, "height", "165"),
    (2, "weight", "60"),
    (2, "age", "25"),
]
df = spark.createDataFrame(data, ["id", "attribute", "value"])
df.show()

# COMMAND ----------

# Pivot from long to wide
solution = df.groupBy("id").pivot("attribute").agg(F.first("value"))
solution.show()
