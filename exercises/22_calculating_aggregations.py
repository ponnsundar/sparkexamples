# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Calculating Aggregations
# MAGIC Perform various aggregation operations on a dataset.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 4500),
    ("Sales", "Charlie", 6000),
    ("Engineering", "Dave", 7000),
    ("Engineering", "Eve", 7500),
    ("Engineering", "Frank", 6500),
    ("Marketing", "Grace", 4000),
    ("Marketing", "Heidi", 4200),
]
df = spark.createDataFrame(data, ["department", "name", "salary"])
df.show()

# COMMAND ----------

# Aggregations per department
solution = df.groupBy("department").agg(
    F.count("*").alias("count"),
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("avg_salary"),
    F.min("salary").alias("min_salary"),
    F.max("salary").alias("max_salary"),
)
solution.show()
