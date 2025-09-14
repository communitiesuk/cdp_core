from pyspark.sql import DataFrame
from databricks.sdk.runtime import *

def delta_writer(
    df: DataFrame,
    catalog: str,
    layer: str,
    table_name: str,
    mode: str,
    partition_by: str = None
) -> None:
    """
    Writes a DataFrame to a Delta table in the specified layer and mode.
    """
    full_table_name = f"{catalog}.{layer}.{table_name}"

    writer = df.write.mode(mode).format("delta")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.saveAsTable(full_table_name)

def add_tags(catalog: str, layer: str, table_name: str, config: dict) -> None:
    """
    Adds Unity Catalog tags to a table and its columns based on the provided config.
    """
    full_table_name = f"{catalog}.{layer}.{table_name}"

    # --- Table-level tags ---
    table_tags = config.get("table_tag", {})
    if table_tags:
        table_tag_string = ", ".join([f"'{key}' = '{value}'" for key, value in table_tags.items()])
        spark.sql(f"""
            ALTER TABLE {full_table_name}
            SET TAGS ({table_tag_string})
        """)

    # --- Column-level tags ---
    schema_config = config.get("schema", {})
    for column, column_config in schema_config.items():
        if "bronze" not in layer:
            column = column_config.get("target", column)

        tags = column_config.get("tag")
        if not tags:
            continue  # Skip if tag is None or empty

        # Ensure tags is a list
        if isinstance(tags, str):
            tags = [tags]

        # Convert list of tags to key-value pairs (key = tag name, value = empty string)
        tag_string = ", ".join([f"'{tag}' = ''" for tag in tags])
        spark.sql(f"""
            ALTER TABLE {full_table_name}
            ALTER COLUMN {column}
            SET TAGS ({tag_string})
        """)


def add_descriptions(catalog: str, layer: str, table_name: str, config: dict) -> None:
    """
    Adds descriptions to a Unity Catalog table and its columns.
    """
    full_table_name = f"{catalog}.{layer}.{table_name}"

    # --- Table Description ---
    table_description = config.get("table_description")
    if table_description:
        spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{table_description}'")

    # --- Column Descriptions ---
    schema_config = config.get("schema", {})
    for column, column_config in schema_config.items():
        if "bronze" not in layer:
            column = column_config.get("target", column)
        description = column_config.get("description")
        if description:
            spark.sql(f"COMMENT ON COLUMN {full_table_name}.{column} IS '{description}'")


def add_permissions(catalog: str, layer: str, table_name: str, config: dict) -> None:
    """
    Adds permissions to a Unity Catalog table.
    """
    full_table_name = f"{catalog}.{layer}.{table_name}"
    
    env = next((e for e in ["test", "prod"] if e in catalog.lower()), "dev")

    permissions = config.get("permissions")

    for environment, role_config in permissions.items():
        if env in environment:
            for principal, privileges in role_config.items():
                spark.sql(f"""
                    GRANT {privileges} ON TABLE {full_table_name}
                    TO ROLE '{principal}'
                """)
            
