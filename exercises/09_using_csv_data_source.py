# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Using CSV Data Source
# MAGIC Load a CSV file with header and inferred schema, then display the content.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

# Create sample CSV data
csv_content = """name,age,city
Alice,30,Seattle
Bob,25,Portland
Charlie,35,San Francisco"""

dbutils.fs.put("/tmp/sample.csv", csv_content, overwrite=True)

# COMMAND ----------

df = spark.read.csv("/tmp/sample.csv", header=True, inferSchema=True)
df.show()
df.printSchema()
