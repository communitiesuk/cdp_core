from urllib.parse import urlencode
from typing import Dict

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from utils.readers import RestClient
from utils.util import config_reader
from utils.writers import delta_writer, add_tags, add_descriptions
from setup.constants import *

spark = SparkSession.getActiveSession()

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

def load(df: DataFrame, config: Dict) -> None:
    table_name = config["dataset"]
    delta_writer(df, CATALOG_SLT1_DEV, SCHEMA_BRONZE, table_name, OVERWRITE)
    add_tags(CATALOG_SLT1_DEV, SCHEMA_BRONZE, table_name, config)
    add_descriptions(CATALOG_SLT1_DEV, SCHEMA_BRONZE, table_name, config)

if __name__ == "__main__":
    dataset = dbutils.widgets.get("dataset")
    runtime = dbutils.widgets.get("runtime")
    config = config_reader(dataset)
    
    df = extract(config)
    df = transform(df)
    load(df, config)