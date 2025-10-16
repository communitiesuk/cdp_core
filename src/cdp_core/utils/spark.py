import os
import importlib
import logging
from pyspark.sql import SparkSession
 
logger = logging.getLogger(__name__)
 
def get_spark():
    """Return a SparkSession appropriate to the execution environment."""
    # 1. Check for Databricks cluster environment (e.g., notebooks or jobs on Databricks)
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):  
        # Running in an actual Databricks cluster environment
        logger.info("Detected Databricks runtime (cluster) environment.")
        return SparkSession.builder.getOrCreate()
    # 2. Not on Databricks cluster; check for Databricks Connect
    databricks_connect_spec = importlib.util.find_spec("databricks.connect")
    if databricks_connect_spec is not None:
        # Databricks Connect library is installed
        try:
            from databricks.connect import DatabricksSession
        except ImportError as ie:
            # The databricks.connect module is present but DatabricksSession is not (incompatible version)
            logger.error("Databricks Connect detected but incompatible version (missing DatabricksSession).")
        else:
            try:
                logger.info("Initializing DatabricksSession via Databricks Connect.")
                return DatabricksSession.builder.getOrCreate()
            except Exception as e:
                # Catch any exception during remote SparkSession creation (e.g., connection issues)
                logger.error(f"Failed to create SparkSession with Databricks Connect: {e}")
                # (Optionally, could fall back to local here if desired, but that might mask issues.)
                raise
 
    # 3. Default to local Spark session
    logger.info("Databricks not detected or not used; falling back to local SparkSession.")
    return SparkSession.builder.master("local[*]").appName("SparkLocalSession").getOrCreate()