# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Reverse-engineering Dataset.show Output
# MAGIC Parse a text file that contains the output of `Dataset.show` back into a DataFrame.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# Simulate a show output as text
show_output = """+---+-----+---+
| id| name|age|
+---+-----+---+
|  1|Alice| 30|
|  2|  Bob| 25|
|  3|Carol| 35|
+---+-----+---+"""

dbutils.fs.put("/tmp/show_output.txt", show_output, overwrite=True)

# COMMAND ----------

# Read as text, filter separator lines, parse columns
raw = spark.read.text("/tmp/show_output.txt")
raw.show(truncate=False)

# COMMAND ----------

# Filter out border lines (starting with +)
data_lines = raw.filter(~F.col("value").startswith("+"))

# Split by | and trim
solution = data_lines.select(
    F.trim(F.split(F.col("value"), "\\|").getItem(1)).alias("id"),
    F.trim(F.split(F.col("value"), "\\|").getItem(2)).alias("name"),
    F.trim(F.split(F.col("value"), "\\|").getItem(3)).alias("age"),
)

# First row is the header — separate it
header = solution.first()
result = solution.filter(
    (F.col("id") != header["id"]) | (F.col("name") != header["name"])
)
result.show()
