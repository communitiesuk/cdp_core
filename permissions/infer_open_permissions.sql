-- set default catalog and schema
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA information_schema;

-- create open tables
CREATE OR REPLACE VIEW open_tables AS
  SELECT catalog_name, schema_name, table_name
  FROM system.information_schema.table_tags
  WHERE 
    (tag_name = 'Access Rights' AND tag_value IN ('OPEN', 'INTERNAL - HMG only','INTERNAL - MHCLG only')) OR 
    (tag_name = 'Contains Personal Identifiable Information' AND tag_value = 'no') OR
    (tag_name = 'Security Classification' AND tag_value = 'OFFICIAL')
  GROUP BY catalog_name, schema_name, table_name
  HAVING
    SUM(CASE WHEN tag_name = 'Access Rights' THEN 1 ELSE 0 END) > 0 AND 
    SUM(CASE WHEN tag_name = 'Contains Personal Identifiable Information' THEN 1 ELSE 0 END) > 0 AND 
    SUM(CASE WHEN tag_name = 'Security Classification' THEN 1 ELSE 0 END) > 0;

-- retrieve tables yet to have open permissions set
CREATE OR REPLACE VIEW open_access_required AS
  SELECT v.catalog_name,v.schema_name,v.table_name
  FROM open_tables v
  LEFT JOIN system.information_schema.table_privileges p
    ON 
      p.table_catalog = v.catalog_name AND 
      p.table_schema  = v.schema_name AND 
      p.table_name    = v.table_name AND 
      p.grantee = 'account users' AND 
      p.privilege_type = 'SELECT'
  WHERE p.privilege_type is NULL;

-- create restricted tables
CREATE OR REPLACE VIEW restricted_tables AS
  SELECT catalog_name, schema_name, table_name
  FROM system.information_schema.table_tags
  WHERE 
    (tag_name = 'Access Rights' AND tag_value NOT IN ('OPEN', 'INTERNAL - HMG only','INTERNAL - MHCLG only')) OR 
    (tag_name = 'Contains Personal Identifiable Information' AND tag_value = 'yes') OR
    (tag_name = 'Security Classification' AND tag_value <> 'OFFICIAL')
  GROUP BY catalog_name, schema_name, table_name;

-- retrieve tables yet to have permissions restricted
CREATE OR REPLACE VIEW close_restricted_tables AS
  SELECT v.catalog_name,v.schema_name,v.table_name
  FROM restricted_tables v
  JOIN system.information_schema.table_privileges p
    ON 
      p.table_catalog = v.catalog_name AND 
      p.table_schema  = v.schema_name AND 
      p.table_name    = v.table_name AND 
      p.grantee = 'account users' AND 
      p.privilege_type = 'SELECT';