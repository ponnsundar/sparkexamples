# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Using Dataset.flatMap Operator
# MAGIC Use flatMap to expand rows — split sentences into individual words.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F, Row

# COMMAND ----------

data = [("Hello World",), ("Apache Spark is great",), ("Databricks rocks",)]
df = spark.createDataFrame(data, ["sentence"])
df.show(truncate=False)

# COMMAND ----------

# flatMap equivalent in PySpark: use explode + split
solution = df.select(F.explode(F.split("sentence", " ")).alias("word"))
solution.show()

# COMMAND ----------

# Alternative using RDD flatMap
rdd_solution = df.rdd.flatMap(lambda row: row.sentence.split(" "))
spark.createDataFrame(rdd_solution.map(lambda w: Row(word=w))).show()
