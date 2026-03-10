# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Merging Two Rows
# MAGIC Merge two rows of the same id to replace nulls with actual values.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("100", "John", 35, None),
    ("100", "John", None, "Georgia"),
    ("101", "Mike", 25, None),
    ("101", "Mike", None, "New York"),
    ("103", "Mary", 22, None),
    ("103", "Mary", None, "Texas"),
    ("104", "Smith", 25, None),
    ("105", "Jake", None, "Florida"),
]
df = spark.createDataFrame(data, ["id", "name", "age", "city"])
df.show()

# COMMAND ----------

# Group by id and name, take first non-null for each column
solution = df.groupBy("id", "name").agg(
    F.first("age", ignorenulls=True).alias("age"),
    F.first("city", ignorenulls=True).alias("city"),
)
solution.orderBy("id").show(truncate=False)
