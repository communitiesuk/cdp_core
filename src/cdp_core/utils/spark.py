def get_spark():
    """
    Return a SparkSession appropriate to the current environment.

    Strategy:
      1) Try Databricks Connect v2 (works in Databricks and locally via Spark Connect).
         If Connect is installed but misconfigured (e.g., version mismatch), convert
         the opaque error into a clear message and attempt local PySpark.
      2) Fallback to local PySpark.
      3) If both fail, raise a concise, actionable RuntimeError.
    """
    # 1) Databricks Connect path
    try:
        from databricks.connect import DatabricksSession
        try:
            return DatabricksSession.builder.getOrCreate()
        except Exception as e:
            # Connect is present but failed (common: version mismatch)
            # Try local PySpark next; if that also fails, wrap with a clear message.
            try:
                from pyspark.sql import SparkSession
                return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
            except Exception as e2:
                raise RuntimeError(
                    "Databricks Connect is installed but failed to start (often a "
                    "version mismatch between your Connect library and the target "
                    "Databricks Runtime). Either:\n"
                    "  • Align versions (upgrade/downgrade databricks-connect), or\n"
                    "  • Run inside a Databricks workspace, or\n"
                    "  • Install/enable local PySpark + Java.\n"
                    "Original error (Connect): " + str(e)
                ) from e2
    except ImportError:
        pass  # Connect not installed → try local PySpark

    # 2) Local PySpark path
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except Exception as e:
        # 3) Friendly failure
        raise RuntimeError(
            "Unable to create a Spark session.\n"
            "Neither Databricks Connect nor local PySpark is usable.\n"
            "To fix:\n"
            "  • For Databricks Connect: install and configure matching versions, set "
            "DATABRICKS_HOST/TOKEN; or\n"
            "  • For local PySpark: install Java (8/11), and `pip install pyspark`."
        ) from e
