# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Difference in Days Between Dates As Strings
# MAGIC Calculate the number of days between two date strings.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    ("2018-03-15", "2018-03-20"),
    ("2018-01-01", "2018-12-31"),
    ("2020-02-28", "2020-03-01"),
]
df = spark.createDataFrame(data, ["start_date", "end_date"])
df.show()

# COMMAND ----------

solution = df.withColumn(
    "diff_days",
    F.datediff(F.to_date("end_date"), F.to_date("start_date"))
)
solution.show()
