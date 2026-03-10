# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Calculating Percent Rank
# MAGIC Use the percent_rank window function to calculate relative ranking within groups.
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
    ("Sales", "Diana", 5500),
    ("Engineering", "Dave", 7000),
    ("Engineering", "Eve", 7500),
    ("Engineering", "Frank", 6500),
]
df = spark.createDataFrame(data, ["department", "name", "salary"])
df.show()

# COMMAND ----------

w = Window.partitionBy("department").orderBy("salary")

solution = df.withColumn("percent_rank", F.percent_rank().over(w))
solution.show()
