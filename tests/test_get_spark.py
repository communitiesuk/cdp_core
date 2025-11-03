import pytest
import sys
import types
import importlib
import builtins
import os
from unittest.mock import MagicMock
from cdp_core.utils.spark import get_spark

"""
Note: Databricks pre-populates sys.modules before you get a chance to run any import hooks, so the only reliable way to intercept imports inside Databricks is to directly replace entries in sys.modules.
That’s effectively monkeypatching the Python import cache itself.

"""
# --------------------------------------------------------------------------- #
# Dummy Spark class for mock testing
# --------------------------------------------------------------------------- #
class DummySpark:
    def __init__(self, name):
        self.name = name
# --------------------------------------------------------------------------- #
# Helper — build a fake Databricks Connect module
# --------------------------------------------------------------------------- #
def make_fake_connect(dummy_spark_name):
    """Create a fake databricks.connect module with the same API surface."""
    dummy_spark = DummySpark(dummy_spark_name)
    fake_builder = MagicMock()
    fake_builder.getOrCreate.return_value = dummy_spark
    class DummyDatabricksSession:
        builder = fake_builder
        @staticmethod
        def getActiveSession():
            # Databricks Connect sometimes checks this before builder.getOrCreate
            return None
    fake_module = types.SimpleNamespace(DatabricksSession=DummyDatabricksSession)
    fake_module.__spec__ = importlib.machinery.ModuleSpec(
        name="databricks.connect", loader=None
    )
    return fake_module
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
# 1. Databricks Connect available (v2)
# --------------------------------------------------------------------------- #
def test_databricks_connect(monkeypatch):
    """Simulate environment where Databricks Connect is installed and usable."""
    sys.modules.pop("databricks.connect", None)
    sys.modules["databricks.connect"] = make_fake_connect("connect")

    spark = get_spark()
    assert spark.name == "connect", f"Expected 'connect', got '{spark.name}'"
# --------------------------------------------------------------------------- #
# 2. Databricks cluster (existing Spark)
# --------------------------------------------------------------------------- #
def test_databricks_cluster_existing_spark(monkeypatch):
    """Simulate Databricks cluster with preexisting Spark session."""
    sys.modules.pop("databricks.connect", None)
    # Simulate allocation of existing Spark session in Databricks Runtime
    sys.modules["databricks.connect"] = make_fake_connect("cluster_existing")

    spark = get_spark()
    assert spark.name == "cluster_existing", (
        f"Expected 'cluster_existing', got '{spark.name}'"
    )
# --------------------------------------------------------------------------- #
# 3. Local fallback (PySpark available, Databricks Connect blocked)
# --------------------------------------------------------------------------- #
def test_local_fallback(monkeypatch):
    """Test that PySpark fallback is used when Connect is unavailable."""
    in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if in_databricks:
        # Inside Databricks: expect a real SparkSession
        from pyspark.sql import SparkSession
        spark = get_spark()
        assert isinstance(spark, SparkSession), \
            f"Expected SparkSession in Databricks runtime, got {type(spark)}"
    else:
        # Local/CI: simulate missing Connect, expect DummySpark("local")
        real_import = builtins.__import__

        def blocked_connect_import(name, *args, **kwargs):
            if name.startswith("databricks.connect"):
                raise ImportError("blocked by test")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocked_connect_import)

        dummy_spark = DummySpark("local")
        fake_builder = MagicMock()
        fake_builder.getOrCreate.return_value = dummy_spark

        class FakeSparkSession:
            builder = fake_builder
            @staticmethod
            def getActiveSession():
                return None

        fake_sql = types.SimpleNamespace(SparkSession=FakeSparkSession)
        sys.modules["pyspark"] = types.SimpleNamespace(sql=fake_sql)
        sys.modules["pyspark.sql"] = fake_sql
        sys.modules["pyspark.sql.SparkSession"] = FakeSparkSession

        spark = get_spark()
        assert spark.name == "local", f"Expected DummySpark('local'), got {getattr(spark, 'name', type(spark))}"

# --------------------------------------------------------------------------- #
# 4. Graceful failure (neither Connect nor PySpark available)
# --------------------------------------------------------------------------- #
def test_graceful_failure(monkeypatch):
    """Raise RuntimeError locally, use live SparkSession in Databricks."""
    in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if in_databricks:
        # In Databricks, PySpark is already available → must get SparkSession
        from pyspark.sql import SparkSession
        spark = get_spark()
        assert isinstance(spark, SparkSession), (
            f"Expected SparkSession in Databricks runtime, got {type(spark)}"
        )
    else:
        # Local / CI: block both Connect and PySpark → expect RuntimeError
        real_import = builtins.__import__

        def blocked_import(name, *args, **kwargs):
            if name.startswith("databricks.connect") or name.startswith("pyspark"):
                raise ImportError("blocked by test")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocked_import)

        with pytest.raises(RuntimeError) as excinfo:
            get_spark()
        msg = str(excinfo.value)
        assert "Unable to create a Spark session" in msg, \
            f"Unexpected error message: {msg}"
    