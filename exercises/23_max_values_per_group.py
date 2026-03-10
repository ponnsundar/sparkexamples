# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Finding Maximum Values Per Group (groupBy)
# MAGIC Find the row with the maximum value in each group.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    ("Sales", "Alice", 5000),
    ("Sales", "Bob", 6000),
    ("Engineering", "Dave", 7000),
    ("Engineering", "Eve", 7500),
    ("Marketing", "Grace", 4000),
    ("Marketing", "Heidi", 4200),
]
df = spark.createDataFrame(data, ["department", "name", "salary"])
df.show()

# COMMAND ----------

# Approach 1: groupBy + join
max_per_dept = df.groupBy("department").agg(F.max("salary").alias("max_salary"))
solution1 = df.join(max_per_dept, ["department"]).filter(F.col("salary") == F.col("max_salary")).drop("max_salary")
solution1.show()

# COMMAND ----------

# Approach 2: window function
w = Window.partitionBy("department").orderBy(F.col("salary").desc())
solution2 = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
solution2.show()
