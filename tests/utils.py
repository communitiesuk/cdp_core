from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def check_if_table_exists(spark_session: SparkSession, catalog_name: str, schema_name: str, table_name: str) -> bool:
    return spark_session.catalog.tableExists(f"{catalog_name}.{schema_name}.{table_name}")

def get_table_schema(spark_session: SparkSession, catalog_name: str, schema_name: str, table_name: str) -> StructType:
    return spark_session.table(f"{catalog_name}.{schema_name}.{table_name}").schema