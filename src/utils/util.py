from pathlib import Path
import yaml
import glob
from typing import Dict
from setup.constants import TYPE_MAPPING
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def config_reader(dataset: str) -> Dict:
    """
    Reads a YAML configuration file for the specified dataset.
    """
    config_dir = Path(__file__).resolve().parents[1] / "configs"

    config_files = glob.glob(str(config_dir / "*"))

    matching_files = [file for file in config_files if dataset in file]
    if not matching_files:
        raise FileNotFoundError(f"No configuration file found for dataset: {dataset}")

    config_path = Path(matching_files[0])

    with config_path.open("r", encoding="utf-8") as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML file: {config_path}") from e


def de_dupe(df: DataFrame, primary_key: str, de_dupe_col: str, de_dupe_asc: bool = True) -> DataFrame:
    dedupe_logic = F.col(de_dupe_col).asc() if de_dupe_asc else F.col(de_dupe_col).desc()
    window_spec = Window.partitionBy(primary_key).orderBy(dedupe_logic)

    return df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter("row_num = 1") \
        .drop("row_num")


def cast_columns(df: DataFrame, config: dict) -> DataFrame:
    schema_config = config.get("schema", {})
    for column, column_config in schema_config.items():
        column_type = column_config.get("type")
        df = df.withColumn(column, F.col(column).cast(TYPE_MAPPING[column_type])) 
    
    return df

def rename_columns(df: DataFrame, config: dict) -> DataFrame:
    schema_config = config.get("schema", {})
    for column, column_config in schema_config.items():
        new_name = column_config.get("target")
        if new_name:
            df = df.withColumnRenamed(column, new_name) 
    return df






