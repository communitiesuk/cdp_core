import pytest
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import DataFrame

from cdp_core.setup.constants import *
from cdp_core.utils.util import config_reader
from cdp_core.transform.silver.silver_arcgis_lad_apr_2023_uk_nc import *
from tests.utils import *


TABLE = "lad_apr_2023_uk_nc_tmp"
DATASET = "LAD_APR_2023_UK_NC"
CATALOG = "`catalog-dev-uks-corecdp-001`"


def test_table_exists(spark):
    execute(DATASET, CATALOG)

    assert check_if_table_exists(spark, CATALOG, SCHEMA_SILVER, TABLE) is True, f"INVALID TABLE"

def test_schema_correct(spark):
    expected_schema = StructType([
        StructField("local_authority_district_code_2023", StringType(), True),
        StructField("local_authority_district_name_2023", StringType(), True)
    ])

    actual_schema = get_table_schema(spark, CATALOG, SCHEMA_SILVER, TABLE)
    assert str(actual_schema) == str(expected_schema), f"Schema does not match. Expected: {expected_schema}, Actual: {actual_schema}"