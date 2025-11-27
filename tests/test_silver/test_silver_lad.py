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
    SCHEMA_SILVER
)
from cdp_core.transform.silver.silver_arcgis_lad_apr_2023_uk_nc import execute

TABLE = "lad_apr_2023_uk_nc"
DATASET = "LAD_APR_2023_UK_NC"

@pytest.mark.order(3)
def test_table_exists(spark):
    execute(DATASET)

    assert check_if_table_exists(spark, CATALOG, SCHEMA_SILVER, TABLE) is True, f"INVALID TABLE"

@pytest.mark.order(4)
def test_schema_correct(spark):
    expected_schema = StructType([
        StructField("local_authority_district_code_2023", StringType(), True),
        StructField("local_authority_district_name_2023", StringType(), True)
    ])

    actual_schema = get_table_schema(spark, CATALOG, SCHEMA_SILVER, TABLE)
    assert str(actual_schema) == str(expected_schema), f"Schema does not match. Expected: {expected_schema}, Actual: {actual_schema}"