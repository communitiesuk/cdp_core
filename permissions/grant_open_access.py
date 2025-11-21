import sys

catalog_name = sys.argv[1]

df = spark.sql(
    f"""
        SELECT catalog_name, schema_name, table_name
        FROM {catalog_name}.information_schema.open_access_required
    """
)

for row in df.collect():
    grant_sql = (
        f"""
        GRANT SELECT ON `{row['catalog_name']}`.`{row['schema_name']}`.`{row['table_name']}` 
        TO `account users`
        """
    )
    print(grant_sql)
    spark.sql(grant_sql)
