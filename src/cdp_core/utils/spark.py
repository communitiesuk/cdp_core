import os
import importlib
import logging
import sys
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)

def get_spark():
    """Return a SparkSession appropriate to the execution environment."""
    logger.info("=" * 70)
    logger.info("ENTERING get_spark()")
    logger.info(f"Module file: {__file__}")
    logger.info(f"Python PID: {os.getpid()}")
    logger.info(f"sys.path[0]: {sys.path[0]}")
    logger.info(f"Environment keys: {list(os.environ.keys())[:20]}...")

    # Log Databricks-related env vars explicitly
    for key in [
        "DATABRICKS_RUNTIME_VERSION",
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "FORCE_LOCAL_SPARK",
    ]:
        logger.info(f"{key}={os.environ.get(key)}")

    # Log if an active Spark session already exists
    try:
        active = SparkSession.getActiveSession()
        logger.info(f"Active SparkSession before logic: {active}")
    except Exception as e:
        logger.warning(f"SparkSession.getActiveSession() failed: {e}")

    # 1️⃣ Databricks runtime
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        logger.info("Detected Databricks runtime environment.")
        spark = SparkSession.builder.getOrCreate()
        logger.info(f"Returning SparkSession (cluster mode): {spark}")
        return spark

    # 2️⃣ Databricks Connect
    databricks_connect_spec = importlib.util.find_spec("databricks.connect")
    logger.info(f"databricks_connect_spec={databricks_connect_spec}")
    if databricks_connect_spec is not None:
        try:
            from databricks.connect import DatabricksSession
            logger.info("Databricks Connect import succeeded.")
            spark = DatabricksSession.builder.getOrCreate()
            logger.info(f"Returning SparkSession (connect mode): {spark}")
            return spark
        except ImportError as e:
            logger.error(f"Databricks Connect import failed: {e}")

    # 3️⃣ Check for preexisting Spark session even if env var missing
    try:
        existing = SparkSession.getActiveSession()
        if existing:
            logger.warning("Existing SparkSession found despite missing env vars!")
            return existing
    except Exception as e:
        logger.warning(f"Error checking existing SparkSession: {e}")

    # 4️⃣ Fallback
    logger.warning("Local Spark not supported in this environment. Returning None.")
    return None