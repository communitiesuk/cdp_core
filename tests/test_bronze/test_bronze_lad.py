import pytest
from pyspark.sql.types import StructType, StructField, StringType

from tests.utils import *
from cdp_core.transform.bronze.bronze_arcgis import *

DATASET = "LAD_APR_2023_UK_NC"
TABLE = "lad_apr_2023_uk_nc_tmp"
CATALOG = "`catalog-dev-uks-corecdp-001`"
SCHEMA = "`schema-dev-uks-corecdp-bronze-001`"

def test_table_exists(spark):
    execute(DATASET, CATALOG)
    assert check_if_table_exists(spark, CATALOG, SCHEMA, TABLE) is True, f"INVALID TABLE"

def test_schema_correct(spark):
    expected_schema = StructType([
        StructField("LAD23CD", StringType(), True),
        StructField("LAD23NM", StringType(), True)
    ])

    actual_schema = get_table_schema(spark, CATALOG, SCHEMA, TABLE)
    assert str(actual_schema) == str(expected_schema), f"Schema does not match. Expected: {expected_schema}, Actual: {actual_schema}"