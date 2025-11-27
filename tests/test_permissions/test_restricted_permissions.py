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
    SCHEMA_SILVER,
    SCHEMA_INFORMATION
)
from apply_permissions import apply_permissions

close_view = "restricted_tables"
close_restricted_view = "close_restricted_tables"
table_name = "lad_apr_2023_uk_nc"

@pytest.mark.order(10)
def test_view_close_creation(spark, permission_script_path):
    # alter PII tags
    for schema in [SCHEMA_BRONZE, SCHEMA_SILVER]:
        spark.sql(f"""
            ALTER TABLE {CATALOG}.{schema}.{table_name} 
            SET TAGS ('Contains Personal Identifiable Information' = 'yes')
        """)

    # run the infer sql script
    execute_sql(spark, CATALOG, permission_script_path)
    
    # assert all closed views are created
    for view in [close_view, close_restricted_view]:
        assert check_if_table_exists(spark, CATALOG, SCHEMA_INFORMATION, view) is True

@pytest.mark.order(11)
def test_view_close_schema(spark):
    # define schema
    expected_schema = StructType([
        StructField("catalog_name", StringType(), False),
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
    ])

    # assert schema matches
    for view in [close_view, close_restricted_view]:
        actual_schema = get_table_schema(spark, CATALOG, SCHEMA_INFORMATION, view)
        assert str(actual_schema) == str(expected_schema), f"Schema does not match. Expected: {expected_schema}, Actual: {actual_schema}"

@pytest.mark.order(12)
def test_view_close_tables(spark):
    df = spark.table(f"{CATALOG}.information_schema.{close_view}")

    results = df.select("table_name").drop_duplicates().collect()

    # assert only one table is defined as close
    assert len(results) == 1

    # assert that the table is lad_apr_2023_uk_nc
    assert results[0]["table_name"] == table_name

@pytest.mark.order(13)
def test_view_required_close_tables(spark):
    df = spark.table(f"{CATALOG}.information_schema.{close_restricted_view}")

    results = df.select("table_name").drop_duplicates().collect()

    # assert only one table is defined as open
    assert len(results) == 1

    # assert that the table is lad_apr_2023_uk_nc
    assert results[0]["table_name"] == table_name

@pytest.mark.order(14)
def test_apply_revoke_permissions(spark):
    # run the apply permission script
    apply_permissions(CATALOG, close_restricted_view, "REVOKE")

    # get updated table permissions
    df = get_table_permissions(spark, CATALOG, SCHEMA_BRONZE, table_name)
    
    # assert account users have SELECT privilege
    assert (
    df.filter(
        (df.grantee == "account users") & (df.privilege_type == "SELECT")
    ).count() == 0
    ), "Table still contains SELECT permission for account users"