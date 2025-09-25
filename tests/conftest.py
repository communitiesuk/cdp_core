import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("pytest").getOrCreate()

    yield spark
    # Cleanup code to delete all tables after tests
    # spark.sql(f"USE CATALOG {CATALOG_SLT1_DEV}")
    # spark.catalog.setCurrentDatabase(SCHEMA_BRONZE)
    # tables = spark.catalog.listTables()
    # for table in tables:
    #     spark.sql(f"DROP TABLE IF EXISTS {table.name}")
