# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Your First Standalone Structured Streaming Application
# MAGIC A basic structured streaming app that reads from a rate source and writes to console/memory.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Rate source generates rows with (timestamp, value) at a steady rate
stream_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
)

# COMMAND ----------

# Add a simple transformation
transformed = stream_df.withColumn(
    "even", F.col("value") % 2 == 0
)

# COMMAND ----------

# Write to a memory sink for interactive querying (Databricks display also works)
query = (
    transformed.writeStream
    .format("memory")
    .queryName("rate_stream")
    .outputMode("append")
    .start()
)

# COMMAND ----------

import time
time.sleep(10)  # Let some data accumulate

spark.sql("SELECT * FROM rate_stream ORDER BY timestamp DESC LIMIT 20").show()

# COMMAND ----------

query.stop()
