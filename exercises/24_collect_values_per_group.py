# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Collect Values Per Group
# MAGIC Collect all values into a list for each group using collect_list and collect_set.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("Sales", "Alice"),
    ("Sales", "Bob"),
    ("Sales", "Alice"),
    ("Engineering", "Dave"),
    ("Engineering", "Eve"),
    ("Marketing", "Grace"),
]
df = spark.createDataFrame(data, ["department", "name"])
df.show()

# COMMAND ----------

solution = df.groupBy("department").agg(
    F.collect_list("name").alias("all_names"),
    F.collect_set("name").alias("unique_names"),
)
solution.show(truncate=False)
