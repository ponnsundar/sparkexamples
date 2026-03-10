# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Using Pivot for Cost Average and Collecting Values
# MAGIC Pivot data to compute averages and collect values per category.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("Product A", "Q1", 100),
    ("Product A", "Q2", 150),
    ("Product A", "Q1", 120),
    ("Product B", "Q1", 200),
    ("Product B", "Q2", 250),
    ("Product B", "Q2", 230),
]
df = spark.createDataFrame(data, ["product", "quarter", "cost"])
df.show()

# COMMAND ----------

# Pivot with average cost
avg_pivot = df.groupBy("product").pivot("quarter").agg(F.avg("cost").alias("avg_cost"))
avg_pivot.show()

# COMMAND ----------

# Pivot with collected values
collect_pivot = df.groupBy("product").pivot("quarter").agg(F.collect_list("cost").alias("costs"))
collect_pivot.show(truncate=False)
