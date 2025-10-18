import pytest
import sys
import types
import importlib
import builtins
import os
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
    """Ensure each test runs in a clean environment before each case."""
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
# 1. Databricks Connect available (v2)
# --------------------------------------------------------------------------- #
def test_databricks_connect(monkeypatch):
    """Simulate environment where Databricks Connect is installed and usable."""
    # Build fake Databricks Connect module before any import
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

    # Block PySpark so we force the Connect path
    real_import = builtins.__import__

    def blocked_pyspark(name, *args, **kwargs):
        if name.startswith("pyspark"):
            raise ImportError("blocked by test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_pyspark)

    spark = get_spark()
    assert spark.name == "connect", f"Expected 'connect', got '{spark.name}'"


# --------------------------------------------------------------------------- #
# 2. Databricks cluster (existing Spark)
# --------------------------------------------------------------------------- #
def test_databricks_cluster_existing_spark(monkeypatch):
    """Simulate Databricks cluster with preexisting Spark session."""
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
        f"Expected 'cluster_existing', got '{spark.name}'"
    )


# --------------------------------------------------------------------------- #
# 3. Local fallback (PySpark available, Databricks Connect blocked)
# --------------------------------------------------------------------------- #
def test_local_fallback(monkeypatch):
    """Simulate local PySpark when Databricks Connect is unavailable.

    • Locally / CI: Databricks Connect blocked → DummySpark("local")
    • Databricks:   Connect preloaded → real SparkSession returned
    """
    real_import = builtins.__import__

    def blocked_connect_import(name, *args, **kwargs):
        if name.startswith("databricks.connect"):
            raise ImportError("blocked by test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_connect_import)

    # Fake PySpark for local case
    dummy_spark = DummySpark("local")
    fake_sql = types.SimpleNamespace()
    fake_sql.SparkSession = types.SimpleNamespace(builder=MagicMock())
    fake_sql.SparkSession.builder.getOrCreate.return_value = dummy_spark

    sys.modules["pyspark"] = types.SimpleNamespace(sql=fake_sql)
    sys.modules["pyspark.sql"] = fake_sql
    sys.modules["pyspark.sql.SparkSession"] = fake_sql.SparkSession

    spark = get_spark()
    in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if in_databricks:
        from pyspark.sql import SparkSession
        assert isinstance(spark, SparkSession), (
            f"Expected real SparkSession in Databricks, got {type(spark)}"
        )
    else:
        assert hasattr(spark, "name") and spark.name == "local", (
            f"Expected DummySpark('local'), got {getattr(spark, 'name', type(spark))}"
        )


# --------------------------------------------------------------------------- #
# 4. Graceful failure (neither Databricks Connect nor PySpark available)
# --------------------------------------------------------------------------- #
def test_graceful_failure(monkeypatch):
    """Verify RuntimeError locally, or real SparkSession in Databricks."""
    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name.startswith("databricks.connect") or name.startswith("pyspark"):
            raise ImportError("blocked by test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if in_databricks:
        # Databricks preloads PySpark → hook ignored → should return SparkSession
        spark = get_spark()
        from pyspark.sql import SparkSession
        assert isinstance(spark, SparkSession), "Expected live SparkSession in Databricks runtime"
    else:
        # Locally: both imports blocked → friendly RuntimeError
        with pytest.raises(RuntimeError) as excinfo:
            get_spark()
        assert "Unable to create a Spark session" in str(excinfo.value)