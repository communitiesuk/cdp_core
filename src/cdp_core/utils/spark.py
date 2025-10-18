def get_spark():
    """
    Return a SparkSession appropriate to the current environment.

    This function uses the modern Databricks Connect v2 interface to automatically
    create or retrieve a Spark session that works seamlessly in both Databricks and
    non-Databricks environments.

    Behavior:
        • When running inside a Databricks workspace (e.g., notebook, job, or cluster),
          `DatabricksSession.builder.getOrCreate()` transparently returns the
          already-active in-cluster `SparkSession` provided by the Databricks runtime.
          No additional configuration or authentication is required.

        • When running locally (e.g., VS Code, PyCharm, or a CI pipeline) with
          Databricks Connect installed and configured, the same call creates a remote
          Spark Connect session that communicates with the configured Databricks
          workspace using `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.

        • If Databricks Connect is not available, the function falls back to creating
          a standard local `pyspark.sql.SparkSession`, allowing offline Spark
          development or testing.

    This unified interface replaces older environment-detection logic
    (checking `DATABRICKS_RUNTIME_VERSION`, etc.) and ensures the same code
    runs unchanged across both Databricks and local environments.

    Returns:
        pyspark.sql.SparkSession: An active Spark session (either local,
        in-cluster, or remote via Spark Connect).
    """
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
