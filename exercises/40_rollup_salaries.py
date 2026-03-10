# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Using rollup Operator for Total and Average Salaries by Department and Company-Wide
# MAGIC Use rollup to compute subtotals per department and a grand total.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

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

# rollup gives subtotals per department + grand total (null department row)
solution = (
    df.rollup("department")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.avg("salary").alias("avg_salary"),
        F.count("*").alias("count"),
    )
    .orderBy(F.col("department").asc_nulls_last())
)
solution.show()
