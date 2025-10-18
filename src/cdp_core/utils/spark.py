def get_spark():
    """
    Return a SparkSession suitable for both local and Databricks execution.

    This function automatically detects and initialises the appropriate Spark entry point,
    supporting both modern Databricks Connect (v2) and standard PySpark environments.

    Databricks Connect v2 is now **natively integrated** with the Databricks Runtime
    (Spark Connect architecture). This means:
    • When running inside a Databricks cluster or notebook, 
        `from databricks.connect import DatabricksSession` will succeed automatically —
        Databricks bundles the Connect client within the runtime itself.
        In this case, `DatabricksSession.builder.getOrCreate()` simply returns the
        existing in-cluster `SparkSession`.
    • When running locally with a configured Connect client, the same call will
        establish a remote connection to your Databricks workspace.
    • When Connect is unavailable or fails (for example, due to a version mismatch),
        the function transparently falls back to a local PySpark session if available.

    The selection order is therefore:
        1. Databricks Connect (works both in Databricks Runtime and remotely)
        2. Local PySpark
        3. Raise a friendly RuntimeError if neither option is usable

    This design allows the same codebase to run unmodified in Databricks, 
    locally via Connect, or in a standalone PySpark environment.
    """
    # 1) Try Databricks Connect first
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        DatabricksSession = None

    if DatabricksSession:
        try:
            return DatabricksSession.getActiveSession() or DatabricksSession.builder.getOrCreate()
        except Exception as e_connect:
            # If Connect exists but fails, try fallback PySpark
            try:
                from pyspark.sql import SparkSession
                return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
            except Exception as e_pyspark:
                raise RuntimeError(
                    "Databricks Connect is installed but failed to start.\n"
                    "Common cause: version mismatch between your Connect client and "
                    "the target Databricks Runtime.\n\n"
                    "Try one of:\n"
                    "  • Align versions (`pip install -U databricks-connect==<runtime>`)\n"
                    "  • Run inside a Databricks workspace\n"
                    "  • Or use local PySpark + Java\n\n"
                    f"Original Connect error: {e_connect}"
                ) from e_pyspark

    # 2) Try local PySpark
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    except Exception as e_final:
        # 3) Friendly failure if all else fails
        raise RuntimeError(
            "Unable to create a Spark session.\n"
            "Neither Databricks Connect nor local PySpark is usable.\n"
            "To fix:\n"
            "  • For Connect: install and configure matching versions (set DATABRICKS_HOST/TOKEN)\n"
            "  • For local: install Java 8/11 and `pip install pyspark`"
        ) from e_final