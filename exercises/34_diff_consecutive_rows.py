# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Calculating Difference Between Consecutive Rows Per Window
# MAGIC Calculate the difference between consecutive running_total rows over time per department.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    ("Sales", "2024-01-01", 100),
    ("Sales", "2024-01-02", 300),
    ("Sales", "2024-01-03", 450),
    ("Engineering", "2024-01-01", 300),
    ("Engineering", "2024-01-02", 550),
    ("Engineering", "2024-01-03", 950),
]
df = spark.createDataFrame(data, ["department", "date", "running_total"])
df.show()

# COMMAND ----------

w = Window.partitionBy("department").orderBy("date")

solution = df.withColumn(
    "prev_total", F.lag("running_total", 1).over(w)
).withColumn(
    "diff", F.col("running_total") - F.col("prev_total")
)
solution.show()
