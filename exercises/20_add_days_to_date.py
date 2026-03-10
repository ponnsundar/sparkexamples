# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: How to Add Days (as Values of a Column) to Date
# MAGIC Add a variable number of days from a column to a date column.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("2018-01-01", 5),
    ("2018-06-15", 10),
    ("2020-12-25", 7),
]
df = spark.createDataFrame(data, ["date_str", "days_to_add"])
df.show()

# COMMAND ----------

solution = df.withColumn(
    "new_date",
    F.date_add(F.to_date("date_str"), F.col("days_to_add"))
)
solution.show()
