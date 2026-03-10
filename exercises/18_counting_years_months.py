# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Counting Occurrences of Years and Months for 24 Months From Now
# MAGIC Generate 24 months from the current date and count occurrences per year and month.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Generate 24 months from now
df = spark.range(24).select(
    F.add_months(F.current_date(), F.col("id").cast("int")).alias("date")
)
df.show(24)

# COMMAND ----------

solution = (
    df.withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .groupBy("year", "month")
    .count()
    .orderBy("year", "month")
)
solution.show(24)
