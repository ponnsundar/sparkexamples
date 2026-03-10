# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise: Standalone Spark Application to Display Spark SQL Version
# MAGIC Display the version of Spark SQL being used.
# MAGIC
# MAGIC Source: [Jacek Laskowski Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/)

# COMMAND ----------

print(f"Spark version: {spark.version}")
print(f"Spark SQL catalog implementation: {spark.conf.get('spark.sql.catalogImplementation', 'in-memory')}")
