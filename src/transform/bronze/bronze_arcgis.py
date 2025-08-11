from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
import sys
import argparse

user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
sys.path.append(f"/Workspace/Users/{user_email}/cdp_core/src")

from urllib.parse import urlencode
from typing import Dict

import pandas as pd


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
    # dataset = dbutils.widgets.get("dataset")
    parser = argparse.ArgumentParser(description="ETL process")
    parser.add_argument("--dataset", required=True, help="Dataset name")
    args = parser.parse_args()
    config = config_reader(args.dataset)
    
    df = extract(config)
    df = transform(df)
    load(df, config)