# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Exploding Structs Array
# MAGIC Explode a nested JSON with business hours into a flat table with one row per day.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

json_data = """{
  "business_id": "abc",
  "full_address": "random_address",
  "hours": {
    "Monday": {"close": "02:00", "open": "11:00"},
    "Tuesday": {"close": "02:00", "open": "11:00"},
    "Friday": {"close": "02:00", "open": "11:00"},
    "Wednesday": {"close": "02:00", "open": "11:00"},
    "Thursday": {"close": "02:00", "open": "11:00"},
    "Sunday": {"close": "00:00", "open": "11:00"},
    "Saturday": {"close": "02:00", "open": "11:00"}
  }
}"""

# Write temp JSON and read it
dbutils.fs.put("/tmp/input.json", json_data, overwrite=True)
df = spark.read.json("/tmp/input.json")
df.show(truncate=False)

# COMMAND ----------

# Stack the day columns into rows
days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
stack_expr = ", ".join([f"'{d}', hours.{d}.open, hours.{d}.close" for d in days])

solution = df.select(
    "business_id",
    "full_address",
    F.expr(f"stack({len(days)}, {stack_expr}) as (day, open_time, close_time)")
)
solution.show(truncate=False)
