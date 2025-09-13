import sys
from typing import Dict

from pyspark.dbutils import DBUtils
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.getActiveSession()
user_email = DBUtils(spark).notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
sys.path.append(f"/Workspace/Users/{user_email}/cdp_core/src")

from cdp_core.setup.constants import *
from cdp_core.utils.readers import read_table
from cdp_core.utils.writers import delta_writer, add_tags, add_descriptions
from cdp_core.utils.util import cast_columns, config_reader, de_dupe, rename_columns


def extract(config: dict, catalog: str) -> DataFrame:
    env = next((e for e in ["test", "prod"] if e in catalog.lower()), "dev")

    SCHEMA_BRONZE = f"`schema-{env}-uks-corecdp-bronze-001`"

    return read_table(catalog, SCHEMA_BRONZE, config['dataset'])

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

    env = next((e for e in ["test", "prod"] if e in catalog.lower()), "dev")

    SCHEMA_SILVER = f"`schema-{env}-uks-corecdp-silver-001`"
    delta_writer(df, catalog, SCHEMA_SILVER, config["dataset"], config["write_method"])
    add_tags(catalog, SCHEMA_SILVER, config["dataset"], config)
    add_descriptions(catalog, SCHEMA_SILVER, config["dataset"], config)

def execute(dataset: str, catalog: str) -> None:
    config = config_reader(dataset)
    df = extract(config, catalog)
    df = transform(df, config)
    load(df, config, catalog)
    