"""
spark.py
--------------
Provides an environment-aware Spark session builder that works both locally
(using Databricks Connect v14+) and inside Databricks clusters.
"""

from pyspark.sql import SparkSession

def get_spark():
    """
    Returns an environment-appropriate SparkSession.

    - In Databricks workspace: returns the cluster SparkSession.
    - Locally: uses Databricks Connect session via the unified CLI config.
    """
    try:
        import dbruntime  # Present only inside Databricks
        spark = SparkSession.builder.getOrCreate()
        print("Using cluster SparkSession")
    except ModuleNotFoundError:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
        print("Using Databricks Connect session")

    return spark
