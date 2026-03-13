# Databricks notebook source
"""
Hello World notebook — verifies the environment is set up correctly.
"""

# COMMAND ----------
from pyspark.sql import SparkSession

print("Hello, world!")
print("Environment is ready.")

spark = SparkSession.builder.getOrCreate()
spark.sql("SELECT 1").show()
# COMMAND ----------
