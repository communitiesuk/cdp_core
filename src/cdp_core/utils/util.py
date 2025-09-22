import yaml
from typing import (
    Union, 
    List
)
from importlib import resources

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from cdp_core.setup.constants import TYPE_MAPPING

def config_reader(dataset: str) -> dict:
    """Reads a YAML configuration file for the specified dataset."""
    with resources.files("cdp_core.configs").joinpath(f"{dataset}.yml").open("r") as file:
        return yaml.safe_load(file)


def de_dupe(df: DataFrame, primary_key: Union[str, List[str]], de_dupe_col: str, de_dupe_asc: bool = True) -> DataFrame:
    """Remove duplicates from a DataFrame based on primary key(s), keeping the row with the min/max value in de_dupe_col."""
    dedupe_logic = F.col(de_dupe_col).asc() if de_dupe_asc else F.col(de_dupe_col).desc()
    partition_keys = [primary_key] if isinstance(primary_key, str) else primary_key
    window_spec = Window.partitionBy(partition_keys).orderBy(dedupe_logic)

    return df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter("row_num = 1") \
        .drop("row_num")


def cast_columns(df: DataFrame, config: dict) -> DataFrame:
    """Casts DataFrame columns to types specified in the config['schema'] dictionary."""
    schema_config = config.get("schema", {})
    for column, column_config in schema_config.items():
        column_type = column_config.get("type")
        if column_type not in TYPE_MAPPING:
            raise ValueError(f"Unknown type '{column_type}' for column '{column}'")
        df = df.withColumn(column, F.col(column).cast(TYPE_MAPPING[column_type])) 
    
    return df

def rename_columns(df: DataFrame, config: dict) -> DataFrame:
    """
    Renames DataFrame columns based on the config['schema'] dictionary.
    Each column's config can have a 'target' key specifying the new name.
    """
    schema_config = config.get("schema", {})
    for column, column_config in schema_config.items():
        new_name = column_config.get("target")
        if new_name and new_name != column:
            df = df.withColumnRenamed(column, new_name) 
    
    return df






