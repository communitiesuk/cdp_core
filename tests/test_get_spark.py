import pytest
import sys
import types
import importlib
import builtins
from unittest.mock import MagicMock

from cdp_core.utils.spark import get_spark


# --------------------------------------------------------------------------- #
# Dummy Spark class for mock testing
# --------------------------------------------------------------------------- #
class DummySpark:
    def __init__(self, name):
        self.name = name


# --------------------------------------------------------------------------- #
# Fixture — ensure each test runs in a clean environment
# --------------------------------------------------------------------------- #
@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    """Ensure each test runs in a clean environment."""
    for mod in [
        "databricks.connect",
        "pyspark",
        "pyspark.sql",
        "pyspark.sql.session",
    ]:
        sys.modules.pop(mod, None)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)


# --------------------------------------------------------------------------- #
# Databricks Connect present (v2)
# --------------------------------------------------------------------------- #
def test_databricks_connect(monkeypatch):
    """Simulate environment with Databricks Connect available (v2)."""

    real_import = builtins.__import__

    def allowed_import(name, *args, **kwargs):
        # Allow databricks.connect, block pyspark to force Connect path
        if name.startswith("pyspark"):
            raise ImportError("blocked by test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", allowed_import)

    dummy_spark = DummySpark("connect")
    fake_builder = MagicMock()
    fake_builder.getOrCreate.return_value = dummy_spark

    class DummyDatabricksSession:
        builder = fake_builder

    dummy_connect_mod = types.SimpleNamespace(DatabricksSession=DummyDatabricksSession)
    dummy_connect_mod.__spec__ = importlib.machinery.ModuleSpec(
        name="databricks.connect", loader=None
    )
    sys.modules["databricks.connect"] = dummy_connect_mod

    spark = get_spark()
    assert spark.name == "connect", f"Expected 'connect', got '{spark.name}'"


# --------------------------------------------------------------------------- #
# Databricks cluster (existing Spark)
# --------------------------------------------------------------------------- #
def test_databricks_cluster_existing_spark(monkeypatch):
    """Simulate Databricks cluster where a Spark session already exists."""
    real_import = builtins.__import__

    def allowed_import(name, *args, **kwargs):
        # Allow databricks.connect, block pyspark
        if name.startswith("pyspark"):
            raise ImportError("blocked by test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", allowed_import)

    dummy_spark = DummySpark("cluster_existing")
    fake_builder = MagicMock()
    fake_builder.getOrCreate.return_value = dummy_spark

    class DummyDatabricksSession:
        builder = fake_builder

    dummy_connect_mod = types.SimpleNamespace(DatabricksSession=DummyDatabricksSession)
    dummy_connect_mod.__spec__ = importlib.machinery.ModuleSpec(
        name="databricks.connect", loader=None
    )
    sys.modules["databricks.connect"] = dummy_connect_mod

    spark = get_spark()
    assert spark.name == "cluster_existing", (
        f"Expected cluster spark session, got '{spark.name}'"
    )


# --------------------------------------------------------------------------- #
# Local fallback (PySpark available, Databricks Connect blocked)
# --------------------------------------------------------------------------- #
def test_local_fallback(monkeypatch):
    """Simulate local PySpark environment when Databricks Connect is unavailable."""

    real_import = builtins.__import__

    def blocked_connect_import(name, *args, **kwargs):
        # Pretend Databricks Connect is not installed
        if name.startswith("databricks.connect"):
            raise ImportError("blocked by test")
        return real_import(name, *args, **kwargs)

    # Block only Databricks Connect
    monkeypatch.setattr(builtins, "__import__", blocked_connect_import)

    # Mock PySpark SparkSession
    dummy_spark = DummySpark("local")
    fake_sql = types.SimpleNamespace()
    fake_sql.SparkSession = types.SimpleNamespace(builder=MagicMock())
    fake_sql.SparkSession.builder.getOrCreate.return_value = dummy_spark

    sys.modules["pyspark"] = types.SimpleNamespace(sql=fake_sql)
    sys.modules["pyspark.sql"] = fake_sql
    sys.modules["pyspark.sql.SparkSession"] = fake_sql.SparkSession

    spark = get_spark()

    assert hasattr(spark, "name") and spark.name == "local", \
        f"Expected 'local', got '{getattr(spark, 'name', type(spark))}'"


# --------------------------------------------------------------------------- #
# Graceful failure (neither Databricks Connect nor PySpark available)
# --------------------------------------------------------------------------- #
def test_graceful_failure(monkeypatch):
    """Verify RuntimeError or existing Spark session behaviour depending on environment.

    - Locally / CI:  Databricks Connect + PySpark blocked → should raise RuntimeError.
    - Databricks:    PySpark already loaded before hook → returns live SparkSession instead.
    """

    import os
    import builtins

    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        # Block both Databricks Connect and PySpark for this simulation
        if name.startswith("databricks.connect") or name.startswith("pyspark"):
            raise ImportError("blocked by test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_import)

    # Detect if we're running inside an actual Databricks cluster/runtime
    in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if in_databricks:
        # In Databricks, PySpark is preloaded — import hook won't apply.
        spark = get_spark()

        # Expect a real SparkSession instance, not a RuntimeError
        from pyspark.sql import SparkSession
        assert isinstance(spark, SparkSession), (
            "Expected a live SparkSession in Databricks runtime"
        )
    else:
        # Locally or in CI, both imports should be blocked → graceful failure path
        with pytest.raises(RuntimeError) as excinfo:
            get_spark()

        message = str(excinfo.value)
        assert "Unable to create a Spark session" in message, \
            f"Unexpected error message: {message}"