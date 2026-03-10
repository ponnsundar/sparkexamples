# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Calculating Gap Between Current and Highest Salaries Per Department
# MAGIC Use window functions to find the gap between each employee's salary and the department max.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 6000),
    ("Sales", "Charlie", 4500),
    ("Engineering", "Dave", 7000),
    ("Engineering", "Eve", 7500),
    ("Engineering", "Frank", 6500),
]
df = spark.createDataFrame(data, ["department", "name", "salary"])
df.show()

# COMMAND ----------

w = Window.partitionBy("department")

solution = df.withColumn(
    "max_salary", F.max("salary").over(w)
).withColumn(
    "gap", F.col("max_salary") - F.col("salary")
)
solution.show()
