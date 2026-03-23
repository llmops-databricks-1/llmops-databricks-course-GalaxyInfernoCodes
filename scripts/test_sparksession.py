from pyspark.sql import SparkSession

try:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()
except ImportError:
    spark = SparkSession.builder.getOrCreate()

spark.sql("SELECT 1").show()
