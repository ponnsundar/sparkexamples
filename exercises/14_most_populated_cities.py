# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Finding Most Populated Cities Per Country
# MAGIC Find the most populated city in each country. Population is a string with spaces.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    ("Warsaw", "Poland", "1 764 615"),
    ("Cracow", "Poland", "769 498"),
    ("Paris", "France", "2 206 488"),
    ("Villeneuve-Loubet", "France", "15 020"),
    ("Pittsburgh PA", "United States", "302 407"),
    ("Chicago IL", "United States", "2 716 000"),
    ("Milwaukee WI", "United States", "595 351"),
    ("Vilnius", "Lithuania", "580 020"),
    ("Stockholm", "Sweden", "972 647"),
    ("Goteborg", "Sweden", "580 020"),
]
df = spark.createDataFrame(data, ["name", "country", "population"])
df.show(truncate=False)

# COMMAND ----------

# Clean population: remove spaces, cast to int
df_clean = df.withColumn(
    "pop_num",
    F.regexp_replace("population", " ", "").cast("int")
)

# Window to rank by population per country
w = Window.partitionBy("country").orderBy(F.col("pop_num").desc())

solution = (
    df_clean.withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("name", "country", "population")
)
solution.show(truncate=False)
