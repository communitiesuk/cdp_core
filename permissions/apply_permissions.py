import sys

from pyspark.sql import SparkSession

def apply_permissions(catalog_name: str, access_table: str, permission: str) -> None:
    spark = SparkSession.builder.appName("permissions").getOrCreate()

    df = spark.sql(
        f"""
            SELECT catalog_name, schema_name, table_name
            FROM {catalog_name}.information_schema.{access_table}
        """
    )

    syntax = "TO" if permission == "GRANT" else "FROM"

    for row in df.collect():
        sql = (
            f"""
            {permission} SELECT ON `{row['catalog_name']}`.`{row['schema_name']}`.`{row['table_name']}` 
            {syntax} `account users`
            """
        )
        print(sql)
        spark.sql(sql)

if __name__ == "__main__":
    catalog_name = sys.argv[1]
    access_table = sys.argv[2]
    permission = sys.argv[3]

    apply_permissions(catalog_name, access_table, permission)