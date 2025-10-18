import pytest
import sys
import types
import importlib
import builtins
import os
from unittest.mock import MagicMock
from src.cdp_core.utils.spark import get_spark
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
    # Block PySpark to force Connect path
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
    sys.modules.pop("databricks.connect", None)
    sys.modules["databricks.connect"] = make_fake_connect("cluster_existing")
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
  • Databricks:  Connect preloaded → real SparkSession returned
  """
  sys.modules.pop("databricks.connect", None)
  real_import = builtins.__import__
  def blocked_connect_import(name, *args, **kwargs):
    if name.startswith("databricks.connect"):
      raise ImportError("blocked by test")
    return real_import(name, *args, **kwargs)
  monkeypatch.setattr(builtins, "__import__", blocked_connect_import)
  # :white_check_mark: Fake PySpark with getActiveSession stub
  dummy_spark = DummySpark("local")
  fake_builder = MagicMock()
  fake_builder.getOrCreate.return_value = dummy_spark
  class FakeSparkSession:
    builder = fake_builder
    @staticmethod
    def getActiveSession():
      # PySpark sometimes calls this first before builder
      return None
  fake_sql = types.SimpleNamespace(SparkSession=FakeSparkSession)
  # Inject into sys.modules so Python imports your fake instead of real PySpark
  sys.modules["pyspark"] = types.SimpleNamespace(sql=fake_sql)
  sys.modules["pyspark.sql"] = fake_sql
  sys.modules["pyspark.sql.SparkSession"] = FakeSparkSession
  # Run
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
    """Verify RuntimeError locally, or SparkSession in Databricks."""
    sys.modules.pop("databricks.connect", None)
    sys.modules.pop("pyspark", None)
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

