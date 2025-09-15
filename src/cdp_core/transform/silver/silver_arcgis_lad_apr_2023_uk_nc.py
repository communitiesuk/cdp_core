import sys
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Dynamically add the repository path to Python's sys.path
# This is primarily useful during development in the Databricks UI.
# In production, this script is typically executed via the .whl entrypoint.
current_user = spark.sql("SELECT current_user()").collect()[0][0]
repo_path = f"/Workspace/Users/{current_user}/cdp_core/src"
if repo_path not in sys.path:
    sys.path.append(repo_path)

# Import CDP Core modules
from cdp_core.setup.constants import *
from cdp_core.utils.readers import read_table
from cdp_core.utils.writers import (
    delta_writer, 
    add_tags, 
    add_descriptions, 
    add_permissions
)
from cdp_core.utils.util import (
    cast_columns, 
    config_reader, 
    de_dupe, 
    rename_columns
)


def extract(config: dict, catalog: str) -> DataFrame:
    env = next((e for e in ["tst", "prd"] if e in catalog.lower()), "dev")

    SCHEMA_BRONZE = f"`schema-{env}-uks-corecdp-bronze-001`"

    return read_table(catalog, SCHEMA_BRONZE, f"{config['dataset']}_tmp")

def transform(df: DataFrame, config: Dict) -> DataFrame:
    # column renaming
    # type casting
    # filtering
    # normalisation
    # deduplication
    # enrichment?
    # validation?
    # partitioning?

    # drop duplicates
    df = df.dropDuplicates()

    # de-duplicate based on primary / composite key
    df = de_dupe(df, config["primary_key"], config["de_dupe_col"])

    # cast to relevant types
    df = cast_columns(df, config)

    # rename columns
    df = rename_columns(df, config)

    return df

def load(df: DataFrame, config: Dict, catalog: str) -> None:
    table_name = f"{config["dataset"]}_tmp"
    env = next((e for e in ["tst", "prd"] if e in catalog.lower()), "dev")

    SCHEMA_SILVER = f"`schema-{env}-uks-corecdp-silver-001`"
    delta_writer(df, catalog, SCHEMA_SILVER, table_name, config["write_method"])
    add_tags(catalog, SCHEMA_SILVER, table_name, config)
    add_descriptions(catalog, SCHEMA_SILVER, table_name, config)
    add_permissions(catalog, SCHEMA_SILVER, table_name, config)

def execute(dataset: str, catalog: str) -> None:
    config = config_reader(dataset)
    df = extract(config, catalog)
    df = transform(df, config)
    load(df, config, catalog)
    