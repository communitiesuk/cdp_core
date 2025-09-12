import os
from pyspark.sql.types import StringType, IntegerType


CATALOG = {
    "dev": "`catalog-dev-uks-corecdp-001`",
    "tst": "`catalog-test-uks-corecdp-001`",
    "prd": "catalog-prod-uks-corecdp-001"
}[os.getenv('env', 'dev')]


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