import pytest
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType
)

from utils import (
    check_if_table_exists, 
    get_table_schema,
    execute_sql,
    get_table_permissions
)

from cdp_core.setup.constants import (
    CATALOG, 
    SCHEMA_BRONZE,
    SCHEMA_INFORMATION
)
from apply_permissions import apply_permissions

open_view = "open_tables"
open_access_view = "open_access_required"
table_name = "lad_apr_2023_uk_nc"

@pytest.mark.order(5)
def test_view_open_creation(spark, permission_script_path):
    # run the infer sql script
    execute_sql(spark, CATALOG, permission_script_path)
    
    # assert all open views are created
    for view in [open_view, open_access_view]:
        assert check_if_table_exists(spark, CATALOG, SCHEMA_INFORMATION, view) is True

@pytest.mark.order(6)
def test_view_open_schema(spark):
    # define schema
    expected_schema = StructType([
        StructField("catalog_name", StringType(), False),
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
    ])

    # assert schema matches
    for view in [open_view, open_access_view]:
        actual_schema = get_table_schema(spark, CATALOG, SCHEMA_INFORMATION, view)
        assert str(actual_schema) == str(expected_schema), f"Schema does not match. Expected: {expected_schema}, Actual: {actual_schema}"

@pytest.mark.order(7)
def test_view_open_tables(spark):
    df = spark.table(f"{CATALOG}.information_schema.{open_view}")

    results = df.select("table_name").drop_duplicates().collect()

    # assert only one table is defined as open
    assert len(results) == 1

    # assert that the table is lad_apr_2023_uk_nc
    assert results[0]["table_name"] == table_name

@pytest.mark.order(8)
def test_view_required_open_tables(spark):
    df = spark.table(f"{CATALOG}.information_schema.{open_access_view}")

    results = df.select("table_name").drop_duplicates().collect()

    # assert only one table is defined as open
    assert len(results) == 1

    # assert that the table is lad_apr_2023_uk_nc
    assert results[0]["table_name"] == table_name

@pytest.mark.order(9)
def test_apply_grant_permissions(spark):
    # run the apply permission script
    apply_permissions(CATALOG, open_access_view, "GRANT")

    # get updated table permissions
    df = get_table_permissions(spark, CATALOG, SCHEMA_BRONZE, table_name)
    
    # assert account users have SELECT privilege
    assert (
    df.filter(
        (df.grantee == "account users") & (df.privilege_type == "SELECT")
    ).count() > 0
    ), "account users does not have SELECT privilege on the table"