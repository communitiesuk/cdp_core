import os

from pyspark.sql.types import StringType, IntegerType

# environment
ENVIRONMENT = os.getenv('env', 'dev')

# catalog
CATALOG =  f"`catalog-{ENVIRONMENT}-uks-corecdp-001`"

# schemas
SCHEMA_BRONZE = f"`schema-{ENVIRONMENT}-uks-corecdp-bronze-001`"
SCHEMA_SILVER = f"`schema-{ENVIRONMENT}-uks-corecdp-silver-001`"
SCHEMA_GOLD = f"`schema-{ENVIRONMENT}-uks-corecdp-gold-001`"

# type mappings
TYPE_MAPPING = {
    "StringType": StringType(),
    "IntegerType": IntegerType()
}