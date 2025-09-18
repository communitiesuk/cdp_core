import sys

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

from cdp_core.utils.logger import get_logger
from cdp_core.utils.readers import read_table
from cdp_core.utils.writers import (
    delta_writer,
    add_metadata
)
from cdp_core.setup.constants import (
    SCHEMA_BRONZE, 
    SCHEMA_SILVER
)
from cdp_core.utils.util import (
    de_dupe, 
    cast_columns, 
    rename_columns,
    config_reader, 
)

# initiate logger instance
logger = get_logger(__name__)

def extract(config: dict) -> DataFrame:
    """Function to host the bronze extraction logic."""
    table_name = f"{config["dataset"]}_tmp" # TODO - remove tmp once table permissions are sorted
    
    return read_table(SCHEMA_BRONZE, table_name)

def transform(df: DataFrame, config: dict) -> DataFrame:
    """Function to host the silver custom transformation logic."""
    # drop duplicates
    df = df.dropDuplicates()

    # de-duplicate based on primary / composite key
    df = de_dupe(df, config["primary_key"], config["de_dupe_col"])

    # cast to relevant types
    df = cast_columns(df, config)

    # rename columns
    df = rename_columns(df, config)

    return df

def load(df: DataFrame, config: dict) -> None:
    """Function to host the silver custom load logic."""
    table_name = f"{config["dataset"]}_tmp" # TODO - remove tmp once table permissions are sorted

    # write to delta table
    delta_writer(df, SCHEMA_SILVER, table_name, config["write_method"])

    # add metadata to table -tags / column description / permissions
    add_metadata(SCHEMA_SILVER, table_name, config)

def execute(dataset: str) -> None:
    """Module entrypoint to execute the extract, transform and load process."""  
    # retrieve dataset config
    config = config_reader(dataset)
    
    # extract, transform and load
    df = extract(config)
    logger.info("Data extracted")

    df = transform(df, config)
    logger.info("Data transformed")
    
    load(df, config)
    logger.info("Data loaded")