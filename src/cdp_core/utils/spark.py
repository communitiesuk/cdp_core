import os
import importlib
import logging
from pyspark.sql import SparkSession  # Commented out to avoid import error if pyspark not installed

logger = logging.getLogger(__name__)

def get_spark():
    """Return a SparkSession appropriate to the execution environment."""
    # 1. Check for Databricks cluster environment (e.g., notebooks or jobs on Databricks)
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):  
        logger.info("Detected Databricks runtime (cluster) environment.")
        # Here you would normally return SparkSession.builder.getOrCreate()
        logger.info("Would create SparkSession on Databricks cluster here.")
        return SparkSession.builder.getOrCreate()

    # 2. Not on Databricks cluster; check for Databricks Connect
    databricks_connect_spec = importlib.util.find_spec("databricks.connect")
    if databricks_connect_spec is not None:
        try:
            from databricks.connect import DatabricksSession
        except ImportError:
            logger.error("Databricks Connect is installed but incompatible (missing DatabricksSession).")
        else:
            logger.info("Using Databricks Connect.")
            return DatabricksSession.builder.getOrCreate()

   # 3. Fallback: local Spark (DISABLED)
    logger.warning("Local Spark not supported in this environment. PySpark and Java required.")
    return None  # or: raise RuntimeError("Local Spark not supported.")

