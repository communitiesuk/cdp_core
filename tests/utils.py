from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

def check_if_table_exists(spark_session: SparkSession, catalog_name: str, schema_name: str, table_name: str) -> bool:
    return spark_session.catalog.tableExists(f"{catalog_name}.{schema_name}.{table_name}")

def get_table_schema(spark_session: SparkSession, catalog_name: str, schema_name: str, table_name: str) -> StructType:
    return spark_session.table(f"{catalog_name}.{schema_name}.{table_name}").schema

def execute_sql(spark_session: SparkSession, catalog_name: str, script_path: str) -> None:
    with open(script_path) as queryFile:
        queryText = queryFile.read()

    for query in queryText.split(";"): 
        query = query.strip()
        if not query:
            continue

        if "IDENTIFIER" in query:
            query = query.replace("IDENTIFIER(:catalog)", catalog_name)
        
        spark_session.sql(query)

def get_table_permissions(spark_session: SparkSession, catalog_name: str, schema_name: str, table_name: str) -> DataFrame:
    catalog_name = catalog_name.replace("`", "")
    schema_name = schema_name.replace("`", "")
    table_name = table_name.replace("`", "")

    return spark_session.sql(
        f"""
            SELECT grantee, privilege_type
            FROM system.information_schema.table_privileges
                WHERE table_catalog = '{catalog_name}'
                AND table_schema = '{schema_name}'
                AND table_name = '{table_name}'
        """
    )