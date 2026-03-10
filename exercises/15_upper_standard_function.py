# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Using upper Standard Function
# MAGIC Convert string columns to uppercase using the `upper` function.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [("hello",), ("world",), ("apache spark",), ("databricks",)]
df = spark.createDataFrame(data, ["text"])
df.show()

# COMMAND ----------

solution = df.withColumn("upper_text", F.upper("text"))
solution.show()
