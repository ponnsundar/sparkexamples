# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Calculating Running Total / Cumulative Sum
# MAGIC Use window functions to compute a running total ordered by time.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    ("Sales", "2024-01-01", 100),
    ("Sales", "2024-01-02", 200),
    ("Sales", "2024-01-03", 150),
    ("Engineering", "2024-01-01", 300),
    ("Engineering", "2024-01-02", 250),
    ("Engineering", "2024-01-03", 400),
]
df = spark.createDataFrame(data, ["department", "date", "amount"])
df.show()

# COMMAND ----------

w = Window.partitionBy("department").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

solution = df.withColumn("running_total", F.sum("amount").over(w))
solution.show()
