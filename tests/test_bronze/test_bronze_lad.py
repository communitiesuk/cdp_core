import pytest
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import DataFrame

from setup.constants import *
from utils.util import config_reader
from transform.bronze.bronze_arcgis import *
from tests.utils import *


TABLE = "lad_apr_2023_uk_nc"
DATASET = "LAD_APR_2023_UK_NC"


def test_table_exists(spark):   
    config = config_reader(DATASET) 
    extracted_df = extract(config)
    transformed_df = transform(extracted_df)
    load(transformed_df, config)

    assert check_if_table_exists(spark, CATALOG_SLT1_DEV, SCHEMA_BRONZE, TABLE) is True, f"INVALID TABLE"

def test_schema_correct(spark):
    expected_schema = StructType([
        StructField("LAD23CD", StringType(), True),
        StructField("LAD23NM", StringType(), True)
    ])

    actual_schema = get_table_schema(spark, CATALOG_SLT1_DEV, SCHEMA_BRONZE, TABLE)
    assert str(actual_schema) == str(expected_schema), f"Schema does not match. Expected: {expected_schema}, Actual: {actual_schema}"