def get_spark():
    """
    Return a SparkSession that works seamlessly in both Databricks and local environments.

    This function automatically detects whether it is running inside a Databricks Runtime
    or a local development environment and initialises the correct Spark entry point.

    Behaviour overview:
    • **Databricks Runtime (cluster or notebook):**
        Databricks Connect v2 is now built into the Databricks Runtime using the Spark Connect
        architecture. Therefore, `from databricks.connect import DatabricksSession` always succeeds,
        and `DatabricksSession.builder.getOrCreate()` simply returns the existing in-cluster
        SparkSession.

    • **Local environment (with Databricks Connect configured):**
        The same call establishes a remote Spark connection to the target Databricks workspace,
        allowing developers to run code locally against live Databricks data.

    • **Local environment (no Connect or Connect misconfigured):**
        If Databricks Connect is unavailable or fails (for example, due to a version mismatch
        between the client and the workspace runtime), the function gracefully falls back
        to a local PySpark session if Java and PySpark are installed.

    Selection order:
        1. Databricks Connect (works both in Databricks and remotely)
        2. Local PySpark
        3. Raise a clear RuntimeError if neither is usable

    This unified approach allows the same codebase to run unmodified in Databricks clusters,
    locally via Databricks Connect, or in a standalone PySpark environment without needing
    environment-specific branching logic.
    """

    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.getActiveSession() or DatabricksSession.builder.getOrCreate()
    except Exception as e_connect:
        # Try local PySpark fallback
        try:
            from pyspark.sql import SparkSession
            return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        except Exception as e_pyspark:
            raise RuntimeError(
                "Unable to create a Spark session.\n"
                "Neither Databricks Connect nor local PySpark could start.\n"
                "Most likely causes:\n"
                "  • Databricks Connect not installed or misconfigured\n"
                "  • PySpark/Java missing locally\n\n"
                f"Original error: {e_connect}"
            ) from e_pyspark