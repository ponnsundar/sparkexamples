# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Using Pivot to Generate a Single-Row Matrix
# MAGIC Use pivot to transform rows into columns, creating a matrix-like output.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("Jan", 100),
    ("Feb", 200),
    ("Mar", 150),
    ("Apr", 300),
    ("May", 250),
]
df = spark.createDataFrame(data, ["month", "sales"])
df.show()

# COMMAND ----------

# Pivot months into columns — single row matrix
solution = df.groupBy().pivot("month").agg(F.first("sales"))
solution.show()
