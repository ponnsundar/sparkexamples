# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Finding 1st and 2nd Bestsellers Per Genre
# MAGIC Use window functions to find the top 2 bestselling books per genre.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    ("Fiction", "Book A", 50000),
    ("Fiction", "Book B", 75000),
    ("Fiction", "Book C", 60000),
    ("Science", "Book D", 40000),
    ("Science", "Book E", 80000),
    ("Science", "Book F", 55000),
    ("History", "Book G", 30000),
    ("History", "Book H", 45000),
]
df = spark.createDataFrame(data, ["genre", "title", "sales"])
df.show()

# COMMAND ----------

w = Window.partitionBy("genre").orderBy(F.col("sales").desc())

solution = (
    df.withColumn("rank", F.row_number().over(w))
    .filter(F.col("rank") <= 2)
    .select("genre", "title", "sales", "rank")
)
solution.show()
