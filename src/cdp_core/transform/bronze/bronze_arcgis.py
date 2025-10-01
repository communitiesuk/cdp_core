import sys
from urllib.parse import urlencode

import pandas as pd
from pyspark.sql import (
    DataFrame, 
    SparkSession
    )

# initialize Spark session
spark = SparkSession.builder.getOrCreate()

# dynamically add the repository path to Python's sys.path
# only useful during development via Databricks UI but is serves no purpose when running via whl
current_user = spark.sql("SELECT current_user()").collect()[0][0]
repo_path = f"/Workspace/Users/{current_user}/cdp_core/src"
if repo_path not in sys.path:
    sys.path.append(repo_path)

from cdp_core.utils.util import config_reader
from cdp_core.utils.logger import get_logger
from cdp_core.utils.readers import RestClient
from cdp_core.utils.writers import (
    delta_writer,
    add_metadata
)
from cdp_core.setup.constants import SCHEMA_BRONZE

# initiate logger instance
logger = get_logger(__name__)

def extract(config: dict) -> DataFrame:
    """"Function to host the source extraction logic."""
    # define endpoint url
    url = f"{config['url']}{config['dataset']}/FeatureServer/0/query?{urlencode(config['params'])}"
    
    # ping rest api endpoint
    client = RestClient(url)
    
    # retrieve response data and convert to pd dataframe    
    resp_json = client.get()['features']
    df = pd.json_normalize(resp_json)
    
    # return spark dataframe
    return spark.createDataFrame(df)

def transform(df: DataFrame) -> DataFrame:
    """Function to host the bronze custom transformation logic."""
    # basic field renaming transformation
    for field in df.columns:
        df = df.withColumnRenamed(field, field.split(".")[-1])    
    return df

def load(df: DataFrame, config: dict) -> None:
    """Function to host the bronze custom load logic."""
    table_name = f"{config['dataset']}_tmp" # TODO - remove tmp once table permissions are sorted

    # write to delta table
    delta_writer(df, SCHEMA_BRONZE, table_name, config["write_method"])

    # add metadata to table - tags / column description / permissions
    add_metadata(SCHEMA_BRONZE, table_name, config)

def execute(dataset: str) -> None:
    """Module entrypoint to execute the extract, transform and load process."""  
    # retrieve dataset config
    config = config_reader(dataset)
    logger.info("Dataset config retrieved")
    
    # extract, transform and load
    df = extract(config)
    logger.info("Data extracted")

    df = transform(df)
    logger.info("Data transformed")
    
    load(df, config)
    logger.info("Data loaded")
