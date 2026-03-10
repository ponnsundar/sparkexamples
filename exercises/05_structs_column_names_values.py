# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Structs for Column Names and Values
# MAGIC Transpose a dataset so column names and values come from a struct column (pivot movie ratings).
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F, Row
from pyspark.sql.types import *

# COMMAND ----------

data = [
    ("Manuel", [("Logan", 1.5), ("Zoolander", 3.0), ("John Wick", 2.5)]),
    ("John", [("Logan", 2.0), ("Zoolander", 3.5), ("John Wick", 3.0)]),
]

schema = StructType([
    StructField("name", StringType()),
    StructField("movieRatings", ArrayType(StructType([
        StructField("movieName", StringType()),
        StructField("rating", DoubleType()),
    ]))),
])

df = spark.createDataFrame(data, schema)
df.show(truncate=False)

# COMMAND ----------

# Explode the array, then pivot on movieName
exploded = df.select("name", F.explode("movieRatings").alias("mr")) \
    .select("name", F.col("mr.movieName").alias("movieName"), F.col("mr.rating").alias("rating"))

solution = exploded.groupBy("name").pivot("movieName").agg(F.first("rating"))
solution.show(truncate=False)
