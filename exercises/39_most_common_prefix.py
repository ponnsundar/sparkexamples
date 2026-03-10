# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Finding Most Common Non-Null Prefix Per Group (Occurrences)
# MAGIC Find the most frequently occurring non-null prefix value per group.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    (1, "abc_1"),
    (1, "abc_2"),
    (1, "xyz_1"),
    (2, "def_1"),
    (2, "def_2"),
    (2, "def_3"),
    (2, "ghi_1"),
    (3, None),
    (3, "jkl_1"),
]
df = spark.createDataFrame(data, ["group_id", "value"])
df.show()

# COMMAND ----------

# Extract prefix (before underscore), filter nulls, count occurrences
df_prefix = (
    df.filter(F.col("value").isNotNull())
    .withColumn("prefix", F.split("value", "_").getItem(0))
)

prefix_counts = df_prefix.groupBy("group_id", "prefix").agg(F.count("*").alias("cnt"))

w = Window.partitionBy("group_id").orderBy(F.col("cnt").desc())
solution = (
    prefix_counts.withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("group_id", "prefix", "cnt")
)
solution.show()
