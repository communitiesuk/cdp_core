from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession
import argparse

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

import sys
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
sys.path.append(f"/Workspace/Users/{user_email}/cdp_core/src")
from typing import Dict

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from utils.writers import delta_writer, add_tags, add_descriptions
from utils.readers import read_table
from utils.util import cast_columns, config_reader, de_dupe, rename_columns
from setup.constants import *
 
# column renaming
# type casting
# filtering
# normalisation
# deduplication
# enrichment?
# validation?
# partitioning?


def extract(config: dict) -> DataFrame:
    return read_table(CATALOG_SLT1_DEV, SCHEMA_BRONZE, config['dataset'])

def transform(df: DataFrame, config: Dict) -> DataFrame:
    # drop duplicates
    df = df.dropDuplicates()

    # de-duplicate based on primary / composite key
    df = de_dupe(df, config["primary_key"], config["de_dupe_col"])

    # cast to relevant types
    df = cast_columns(df, config)

    # rename columns
    df = rename_columns(df, config)

    return df

def load(df: DataFrame, config: Dict) -> None:
    delta_writer(df, CATALOG_SLT1_DEV, SCHEMA_SILVER, config["dataset"], config["write_method"])
    add_tags(CATALOG_SLT1_DEV, SCHEMA_SILVER, config["dataset"], config)
    add_descriptions(CATALOG_SLT1_DEV, SCHEMA_SILVER, config["dataset"], config)

if __name__ == "__main__":
    # dataset = dbutils.widgets.get("dataset")
    parser = argparse.ArgumentParser(description="ETL process")
    parser.add_argument("--dataset", required=True, help="Dataset name")
    args = parser.parse_args()

    config = config_reader(args.dataset)
    
    df = extract(config)
    df = transform(df, config)
    load(df, config)
