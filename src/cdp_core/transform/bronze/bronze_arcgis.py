import sys
from typing import Dict
from urllib.parse import urlencode

import pandas as pd
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.getActiveSession()
user_email = DBUtils(spark).notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
sys.path.append(f"/Workspace/Users/{user_email}/cdp_core/src")

from cdp_core.setup.constants import *
from cdp_core.utils.util import config_reader
from cdp_core.utils.readers import RestClient
from cdp_core.utils.writers import delta_writer, add_tags, add_descriptions


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
    table_name = config["dataset"]
    delta_writer(df, catalog, SCHEMA_BRONZE, table_name, OVERWRITE)
    add_tags(catalog, SCHEMA_BRONZE, table_name, config)
    add_descriptions(catalog, SCHEMA_BRONZE, table_name, config)

def execute(dataset: str, catalog: str) -> None:
    config = config_reader(dataset)
    df = extract(config)
    df = transform(df)
    load(df, config, catalog)
