from pyspark.sql.types import StringType, IntegerType

# catalogs
CATALOG_SLT1_DEV = "dev_service_line_team_1"
CATALOG_SLT1_TEST = "test_service_line_team_1"
CATALOG_SLT1_PROD = "prod_service_line_team_1"

# schemas
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# write mode
OVERWRITE = "overwrite"
APPEND = "append"

# type mappings
TYPE_MAPPING = {
    "StringType": StringType(),
    "IntegerType": IntegerType()
}