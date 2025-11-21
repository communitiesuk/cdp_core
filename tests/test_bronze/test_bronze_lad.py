import pytest
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType
)

from utils import (
    check_if_table_exists, 
    get_table_schema
)
from cdp_core.setup.constants import (
    CATALOG, 
    SCHEMA_BRONZE
)
from cdp_core.transform.bronze.bronze_arcgis import execute

DATASET = "LAD_APR_2023_UK_NC"
TABLE = "lad_apr_2023_uk_nc"

def test_table_exists(spark):
    execute(DATASET)
    assert check_if_table_exists(spark, CATALOG, SCHEMA_BRONZE, TABLE) is True, f"INVALID TABLE"

def test_schema_correct(spark):
    expected_schema = StructType([
        StructField("LAD23CD", StringType(), True),
        StructField("LAD23NM", StringType(), True)
    ])

    actual_schema = get_table_schema(spark, CATALOG, SCHEMA_BRONZE, TABLE)
    assert str(actual_schema) == str(expected_schema), f"Schema does not match. Expected: {expected_schema}, Actual: {actual_schema}"