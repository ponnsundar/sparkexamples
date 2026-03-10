# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Streaming CSV Datasets
# MAGIC Read CSV files as a stream — new files dropped into a directory are picked up automatically.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# Define schema (required for streaming CSV)
schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("city", StringType()),
])

# COMMAND ----------

# Prepare input directory with a sample file
csv_content = """name,age,city
Alice,30,Seattle
Bob,25,Portland"""

dbutils.fs.mkdirs("/tmp/streaming_csv_input")
dbutils.fs.put("/tmp/streaming_csv_input/batch1.csv", csv_content, overwrite=True)

# COMMAND ----------

# Read as a stream
stream_df = (
    spark.readStream
    .format("csv")
    .option("header", True)
    .schema(schema)
    .load("/tmp/streaming_csv_input/")
)

# COMMAND ----------

# Write to memory sink
query = (
    stream_df.writeStream
    .format("memory")
    .queryName("csv_stream")
    .outputMode("append")
    .start()
)

# COMMAND ----------

import time
time.sleep(5)
spark.sql("SELECT * FROM csv_stream").show()

# COMMAND ----------

# Drop another file to see it picked up
csv_content2 = """name,age,city
Charlie,35,Denver
Diana,28,Austin"""
dbutils.fs.put("/tmp/streaming_csv_input/batch2.csv", csv_content2, overwrite=True)

time.sleep(5)
spark.sql("SELECT * FROM csv_stream").show()

# COMMAND ----------

query.stop()
# Cleanup
dbutils.fs.rm("/tmp/streaming_csv_input", recurse=True)
