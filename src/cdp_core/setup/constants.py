from pyspark.sql.types import StringType, IntegerType

# catalogs
CATALOG_SLT1_SBX = "`catalog-sbx-uks-corecdp-001`"
CATALOG_SLT1_DEV = "`catalog-sbx-uks-corecdp-001`" # for development purposes
CATALOG_SLT1_TEST = "`catalog-test-uks-corecdp-001`"
CATALOG_SLT1_PROD = "`catalog-prod-uks-corecdp-001`"

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