# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Generating Exam Assessment Report
# MAGIC Pivot student answers so question IDs become columns and answers become values.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

data = [
    (1, "Question1Text", "Yes", "abcde1", 0, "(x1,y1)"),
    (2, "Question2Text", "No", "abcde1", 0, "(x1,y1)"),
    (3, "Question3Text", "3", "abcde1", 0, "(x1,y1)"),
    (1, "Question1Text", "No", "abcde2", 0, "(x2,y2)"),
    (2, "Question2Text", "Yes", "abcde2", 0, "(x2,y2)"),
]
df = spark.createDataFrame(data, ["Qid", "Question", "AnswerText", "ParticipantID", "Assessment", "GeoTag"])
df.show()

# COMMAND ----------

# Pivot: question IDs become columns
solution = (
    df.withColumn("Qid_col", F.concat(F.lit("Qid_"), F.col("Qid")))
    .groupBy("ParticipantID", "Assessment", "GeoTag")
    .pivot("Qid_col")
    .agg(F.first("AnswerText"))
)
solution.show()
