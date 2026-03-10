# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Why Are All Fields Null When Querying With Schema?
# MAGIC Demonstrate the issue of null fields when schema column names don't match the data.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# Create sample CSV
csv_data = """name,age,city
Alice,30,Seattle
Bob,25,Portland"""

dbutils.fs.put("/tmp/schema_test.csv", csv_data, overwrite=True)

# COMMAND ----------

# Wrong schema (column names don't match CSV headers) — fields will be null
wrong_schema = StructType([
    StructField("first_name", StringType()),
    StructField("years", IntegerType()),
    StructField("location", StringType()),
])

df_wrong = spark.read.csv("/tmp/schema_test.csv", header=True, schema=wrong_schema)
print("With WRONG schema (mismatched column names):")
df_wrong.show()

# COMMAND ----------

# Correct schema (column names match CSV headers)
correct_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("city", StringType()),
])

df_correct = spark.read.csv("/tmp/schema_test.csv", header=True, schema=correct_schema)
print("With CORRECT schema:")
df_correct.show()
