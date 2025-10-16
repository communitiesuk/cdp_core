import pytest
import sys
import types
from unittest import mock
 
from cdp_core.utils.spark import get_spark
 
 
class DummySpark:
    def __init__(self, name):
        self.name = name
 
    class builder:
        @staticmethod
        def getOrCreate():
            return DummySpark("local")
 
 
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
 
    dummy_cluster_spark = DummySpark("cluster")
    monkeypatch.setattr("pyspark.sql.SparkSession.builder.getOrCreate", lambda: dummy_cluster_spark)
 
    spark = get_spark()
    assert spark.name == "cluster"
 
 
def test_databricks_connect(monkeypatch):
    """Simulate Databricks Connect v14+ (remote Spark via Spark Connect)."""
    # Simulate databricks.connect and DatabricksSession
    dummy_connect_mod = types.SimpleNamespace()
    dummy_session_class = type("DatabricksSession", (), {
        "builder": type("builder", (), {
            "getOrCreate": staticmethod(lambda: DummySpark("connect"))
        })()
    })
    dummy_connect_mod.DatabricksSession = dummy_session_class
    sys.modules["databricks.connect"] = dummy_connect_mod
 
    spark = get_spark()
    assert spark.name == "connect"
 
 
def test_local_fallback(monkeypatch):
    """Simulate fully local environment."""
    monkeypatch.setattr("pyspark.sql.SparkSession.builder.getOrCreate", lambda: DummySpark("local"))
    monkeypatch.setattr("pyspark.sql.SparkSession.builder.master", lambda *_: DummySpark("local"))
    monkeypatch.setattr("pyspark.sql.SparkSession.builder.appName", lambda *_: DummySpark("local"))
 
    spark = get_spark()
    assert spark.name == "local"