from databricks.sdk.runtime import *

from pyspark.sql import DataFrame, SparkSession

from cdp_core.setup.constants import CATALOG, ENVIRONMENT

spark = SparkSession.builder.getOrCreate()

def delta_writer(df: DataFrame, schema: str, table_name: str, mode: str, partition_by: str = None) -> None:
    """ Function to write a Spark DataFrame into a Delta table in the specified schema and mode."""
    full_table_name = f"{CATALOG}.{schema}.{table_name}"

    writer = df.write.mode(mode).format("delta")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.saveAsTable(full_table_name)

def get_full_table_name(schema: str, table_name: str) -> str:
    """Constructs the fully qualified Unity Catalog table name."""
    return f"{CATALOG}.{schema}.{table_name}"

def resolve_column_name(schema: str, column: str, column_config: dict) -> str:
    """
    Resolves the column name based on schema type and config.
    If not bronze, uses the 'target' field from config.
    """
    return column_config.get("target", column) if "bronze" not in schema else column

def apply_table_tags(full_table_name: str, tags: dict) -> None:
    """Applies Unity Catalog tags to the table."""
    if tags:
        tag_str = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
        spark.sql(f"ALTER TABLE {full_table_name} SET TAGS ({tag_str})")

def apply_column_tags(full_table_name: str, schema_config: dict, schema: str) -> None:
    """Applies Unity Catalog tags to individual columns based on schema config."""
    for col, cfg in schema_config.items():
        tags = cfg.get("tag")
        if not tags:
            continue
        if isinstance(tags, str):
            tags = [tags]
        tag_str = ", ".join(f"'{tag}' = ''" for tag in tags)
        resolved_col = resolve_column_name(schema, col, cfg)
        spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN {resolved_col} SET TAGS ({tag_str})")

def apply_descriptions(full_table_name: str, config: dict, schema: str) -> None:
    """Applies table and column descriptions in Unity Catalog."""
    if desc := config.get("table_description"):
        spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{desc}'")
    for col, cfg in config.get("schema", {}).items():
        if desc := cfg.get("description"):
            resolved_col = resolve_column_name(schema, col, cfg)
            spark.sql(f"COMMENT ON COLUMN {full_table_name}.{resolved_col} IS '{desc}'")

def apply_permissions(full_table_name: str, config: dict) -> None:
    """Grants permissions to principals based on environment-specific config."""
    for env, roles in config.get("permissions", {}).items():
        if ENVIRONMENT in env:
            for principal, privileges in roles.items():
                spark.sql(f"GRANT {privileges} ON TABLE {full_table_name} TO `{principal}`")

def add_metadata(schema: str, table_name: str, config: dict) -> None:
    """Applies tags, descriptions, and permissions to a Unity Catalog table and its columns."""
    full_table_name = get_full_table_name(schema, table_name)
    schema_config = config.get("schema", {})

    apply_table_tags(full_table_name, config.get("table_tag", {}))
    apply_column_tags(full_table_name, schema_config, schema)
    apply_descriptions(full_table_name, config, schema)
    apply_permissions(full_table_name, config)



            
