# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Finding First Non-Null Value Per Group
# MAGIC Find the first non-null value in a column for each group, ordered by a timestamp.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    (1, "2024-01-01", None),
    (1, "2024-01-02", "A"),
    (1, "2024-01-03", "B"),
    (2, "2024-01-01", None),
    (2, "2024-01-02", None),
    (2, "2024-01-03", "C"),
]
df = spark.createDataFrame(data, ["id", "date", "value"])
df.show()

# COMMAND ----------

# Approach 1: groupBy with first (ignorenulls)
solution1 = df.groupBy("id").agg(
    F.first("value", ignorenulls=True).alias("first_non_null")
)
solution1.show()

# COMMAND ----------

# Approach 2: window function
w = Window.partitionBy("id").orderBy("date")
solution2 = (
    df.withColumn("first_val", F.first("value", ignorenulls=True).over(w))
    .groupBy("id")
    .agg(F.first("first_val", ignorenulls=True).alias("first_non_null"))
)
solution2.show()
