# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Using UDFs (User-Defined Functions)
# MAGIC Create and apply a UDF to transform data in a DataFrame.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

# COMMAND ----------

# Define a UDF that categorizes age
@F.udf(StringType())
def age_category(age):
    if age is None:
        return "Unknown"
    elif age < 30:
        return "Young"
    elif age < 40:
        return "Middle"
    else:
        return "Senior"

solution = df.withColumn("category", age_category(F.col("age")))
solution.show()

# COMMAND ----------

# Register UDF for SQL usage
spark.udf.register("age_category_sql", age_category)
df.createOrReplaceTempView("people")
spark.sql("SELECT *, age_category_sql(age) AS category FROM people").show()
