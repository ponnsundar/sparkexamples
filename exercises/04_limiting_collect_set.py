# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Limiting collect_set Standard Function
# MAGIC Collect all values per group, then limit to only the first 3 elements.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df = spark.range(50).withColumn("key", F.col("id") % 5)
df.show()

# COMMAND ----------

solution = (
    df.groupBy("key")
    .agg(
        F.collect_set("id").alias("all"),
    )
    .withColumn("only_first_three", F.slice(F.col("all"), 1, 3))
)
solution.show(truncate=False)
