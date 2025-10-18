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
    sys.modules.pop("pyspark", None)
    sys.modules.pop("pyspark.sql", None)
    sys.modules.pop("pyspark.sql.session", None)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)


# --------------------------------------------------------------------------- #
# Databricks Connect present (v2)
# --------------------------------------------------------------------------- #
def test_databricks_connect(monkeypatch):
    """Simulate environment with Databricks Connect available (v2)."""
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
    dummy_connect_mod = types.SimpleNamespace()
    dummy_connect_mod.__spec__ = importlib.machinery.ModuleSpec(
        name="databricks.connect", loader=None
    )
    sys.modules["databricks.connect"] = dummy_connect_mod

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


# --------------------------------------------------------------------------- #
# Local fallback (PySpark available, Databricks Connect missing)
# --------------------------------------------------------------------------- #
def test_local_fallback(monkeypatch):
    """Simulate local PySpark environment when Databricks Connect is unavailable."""
    # Build a fake pyspark module structure
    dummy_spark = DummySpark("local")
    fake_sql = types.SimpleNamespace()
    fake_sql.SparkSession = types.SimpleNamespace(builder=MagicMock())
    fake_sql.SparkSession.builder.getOrCreate.return_value = dummy_spark

    fake_pyspark = types.SimpleNamespace(sql=fake_sql)
    sys.modules["pyspark"] = fake_pyspark
    sys.modules["pyspark.sql"] = fake_sql
    sys.modules["pyspark.sql.SparkSession"] = fake_sql.SparkSession

    # Pretend databricks.connect not installed
    sys.modules.pop("databricks.connect", None)

    spark = get_spark()
    assert spark.name == "local", f"Expected 'local', got '{spark.name}'"


# --------------------------------------------------------------------------- #
# Graceful failure (neither Databricks Connect nor PySpark available)
# --------------------------------------------------------------------------- #
def test_graceful_failure(monkeypatch):
    """Verify friendly RuntimeError is raised when no Spark backend is available."""
    # Simulate no databricks.connect or pyspark installed
    sys.modules.pop("databricks.connect", None)
    sys.modules.pop("pyspark", None)
    sys.modules.pop("pyspark.sql", None)
    sys.modules.pop("pyspark.sql.session", None)

    with pytest.raises(RuntimeError) as excinfo:
        get_spark()

    message = str(excinfo.value)
    assert "Unable to create a local SparkSession" in message
    assert "Java" in message or "PySpark" in message
    