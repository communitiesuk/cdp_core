import pytest
import sys
import types
import os
import importlib
from unittest.mock import MagicMock
 
from cdp_core.utils.spark import get_spark
 
 
class DummySpark:
    def __init__(self, name):
        self.name = name
 
 
@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    # Reset environment between tests
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("FORCE_LOCAL_SPARK", raising=False)
    # Remove dynamic modules
    sys.modules.pop("dbruntime", None)
    sys.modules.pop("databricks.connect", None)
 
 
def test_databricks_cluster(monkeypatch):
    """Simulate Databricks UI/cluster environment."""
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")

    # Your dummy spark object
    dummy_cluster_spark = DummySpark("cluster")

    # Mock the builder object with getOrCreate
    fake_builder = MagicMock()
    fake_builder.getOrCreate.return_value = dummy_cluster_spark

    # Patch the builder attribute on SparkSession
    monkeypatch.setattr("pyspark.sql.SparkSession.builder", fake_builder)

    # Now call your function
    spark = get_spark()

    assert spark.name == "cluster", f"Expected 'cluster', got '{spark.name}'"
 
 
def test_databricks_connect(monkeypatch):
    """Simulate Databricks Connect v14+ (remote Spark via Spark Connect)."""

    dummy_spark = DummySpark("connect")

    fake_builder = MagicMock()
    fake_builder.getOrCreate.return_value = dummy_spark

    class DummyDatabricksSession:
        builder = fake_builder

    dummy_connect_mod = types.SimpleNamespace(
        DatabricksSession=DummyDatabricksSession
    )
    dummy_connect_mod.__spec__ = importlib.machinery.ModuleSpec(
        name="databricks.connect",
        loader=None,
    )

    sys.modules["databricks.connect"] = dummy_connect_mod

    spark = get_spark()
    assert spark.name == "connect", f"Expected 'connect', got '{spark.name}'"
 
def test_local_fallback(monkeypatch):
    """Simulate fully local environment (no Databricks, no Connect)."""
    
    # Replace os.environ with a clean dict
    monkeypatch.setattr(os, "environ", {})
    spark = get_spark()

    assert spark is None, "Expected None for local Spark fallback"