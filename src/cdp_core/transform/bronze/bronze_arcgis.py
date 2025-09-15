import sys
from typing import Dict
from urllib.parse import urlencode

import pandas as pd
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
from cdp_core.utils.util import config_reader
from cdp_core.utils.readers import RestClient
from cdp_core.utils.writers import (
    delta_writer,
    add_tags,
    add_descriptions,
    add_permissions
)


def extract(config: dict) -> DataFrame:
    url = f"{config['url']}{config['dataset']}/FeatureServer/0/query?{urlencode(config['params'])}"

    client = RestClient(url)
    resp_json = client.get()['features']

    df = pd.json_normalize(resp_json)
    return spark.createDataFrame(df)

def transform(df: DataFrame) -> DataFrame:
    for field in df.columns:
        df = df.withColumnRenamed(field, field.split(".")[-1])
    return df

def load(df: DataFrame, config: Dict, catalog: str) -> None:
    table_name = f"{config["dataset"]}_tmp"

    env = next((e for e in ["tst", "prd"] if e in catalog.lower()), "dev")

    SCHEMA_BRONZE = f"`schema-{env}-uks-corecdp-bronze-001`"

    delta_writer(df, catalog, SCHEMA_BRONZE, table_name, OVERWRITE)
    add_tags(catalog, SCHEMA_BRONZE, table_name, config)
    add_descriptions(catalog, SCHEMA_BRONZE, table_name, config)
    add_permissions(catalog, SCHEMA_BRONZE, table_name, config)

def execute(dataset: str, catalog: str) -> None:
    config = config_reader(dataset)
    df = extract(config)
    df = transform(df)
    load(df, config, catalog)
