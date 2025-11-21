import sys

catalog_name = sys.argv[1]

df = spark.sql(
    f"""
        SELECT catalog_name, schema_name, table_name
        FROM {catalog_name}.information_schema.close_restricted_tables
    """
)

for row in df.collect():
    grant_sql = (
        f"""
        REVOKE SELECT ON TABLE `{row['catalog_name']}`.`{row['schema_name']}`.`{row['table_name']}` 
        FROM `account users`
        """
    )
    spark.sql(grant_sql)
    print(grant_sql)
