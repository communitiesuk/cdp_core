from itertools import product

import pytest
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from cdp_core.setup.constants import (
    CATALOG, 
    SCHEMA_BRONZE,
    SCHEMA_SILVER,
    SCHEMA_INFORMATION
)

@pytest.fixture(scope="session")
def permission_script_path():
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[2]

    return f"/Workspace/Users/{user}/.bundle/cdp_core/files/permissions/infer_permissions.sql"


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("pytest").getOrCreate()
    
    yield spark

    # Teardown: drop core created tables  
    schemas = [SCHEMA_BRONZE, SCHEMA_SILVER]
    tables = ["lad_apr_2023_uk_nc"]
    for schema, table in product(schemas, tables):
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{schema}.{table}")

    # Teardown: drop system created views
    views = ["open_tables", "open_access_required", "restricted_tables", "close_restricted_tables"]

    for view in views:
      spark.sql(f"DROP VIEW IF EXISTS {CATALOG}.{SCHEMA_INFORMATION}.{view}")  

