import os

from pyspark.sql.types import StringType, IntegerType

CATALOG = {
    "dev": "`catalog-dev-uks-corecdp-001`",
    "tst": "`catalog-test-uks-corecdp-001`",
    "prd": "`catalog-prod-uks-corecdp-001`"
}[os.getenv('env', 'dev')]

# schemas
SCHEMA_BRONZE = "`schema-dev-uks-corecdp-bronze-001`"
SCHEMA_SILVER = "`schema-dev-uks-corecdp-silver-001`"
SCHEMA_GOLD = "`schema-dev-uks-corecdp-gold-001`"

# write mode
OVERWRITE = "overwrite"
APPEND = "append"

# type mappings
TYPE_MAPPING = {
    "StringType": StringType(),
    "IntegerType": IntegerType()
}