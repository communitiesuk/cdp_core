import pytest
import sys
import types
import importlib
from unittest.mock import MagicMock

from cdp_core.utils.spark import get_spark


class DummySpark:
    def __init__(self, name):
        self.name = name


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    """Ensure each test runs in a clean environment."""
    sys.modules.pop("databricks.connect", None)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)


def test_databricks_connect(monkeypatch):
    """Simulate environment with Databricks Connect available (v2)."""
    dummy_spark = DummySpark("connect")

    fake_builder = MagicMock()
    fake_builder.getOrCreate.return_value = dummy_spark

    class DummyDatabricksSession:
        builder = fake_builder

    dummy_connect_mod = types.SimpleNamespace(DatabricksSession=DummyDatabricksSession)
    dummy_connect_mod.__spec__ = importlib.machinery.ModuleSpec(
        name="databricks.connect",
        loader=None,
    )

    sys.modules["databricks.connect"] = dummy_connect_mod

    spark = get_spark()
    assert spark.name == "connect", f"Expected 'connect', got '{spark.name}'"


def test_databricks_cluster_existing_spark(monkeypatch):
    """Simulate Databricks cluster where `spark` already exists in global scope."""
    # Pretend Databricks Connect module is available (as it is on clusters)
    dummy_connect_mod = types.SimpleNamespace()
    dummy_connect_mod.__spec__ = importlib.machinery.ModuleSpec(
        name="databricks.connect", loader=None
    )
    sys.modules["databricks.connect"] = dummy_connect_mod

    # Mock DatabricksSession to just return the global spark (cluster case)
    dummy_spark = DummySpark("cluster_existing")
    fake_builder = MagicMock()
    fake_builder.getOrCreate.return_value = dummy_spark

    class DummyDatabricksSession:
        builder = fake_builder

    dummy_connect_mod.DatabricksSession = DummyDatabricksSession

    spark = get_spark()
    assert spark.name == "cluster_existing", (
        f"Expected cluster spark session, got '{spark.name}'"
    )


def test_local_fallback(monkeypatch):
    """Simulate environment with no Databricks Connect installed."""
    # Mock importlib.find_spec to simulate missing databricks.connect
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name: None if name == "databricks.connect" else importlib.util.find_spec(name),
    )

    dummy_spark = DummySpark("local")
    fake_builder = MagicMock()
    fake_builder.getOrCreate.return_value = dummy_spark

    monkeypatch.setattr("pyspark.sql.SparkSession.builder", fake_builder)

    spark = get_spark()
    assert spark.name == "local", f"Expected 'local', got '{spark.name}'"
    