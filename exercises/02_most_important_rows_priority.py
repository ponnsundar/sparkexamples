# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Selecting the Most Important Rows Per Assigned Priority
# MAGIC Select the most important (first) row per id based on a priority ordering.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [(1, "MV1"), (1, "MV2"), (2, "VPV"), (2, "Others")]
df = spark.createDataFrame(data, ["id", "value"])
df.show()

# COMMAND ----------

# Define priority: MV* > VPV > Others
priority_order = F.when(F.col("value").startswith("MV"), 1) \
    .when(F.col("value") == "VPV", 2) \
    .otherwise(3)

w = Window.partitionBy("id").orderBy(priority_order)

solution = (
    df.withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", F.col("value").alias("name"))
)
solution.show(truncate=False)
