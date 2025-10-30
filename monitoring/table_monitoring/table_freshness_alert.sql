/*
Run time
{run_time}
{description}
 */

WITH meta AS (
  SELECT
    CONCAT(table_catalog, '.', table_schema, '.', table_name) AS table_id,
    TIMESTAMP(MAX(creation_time)) AS last_modified_utc
  FROM `{project_id}.{dataset}`.INFORMATION_SCHEMA.TABLES
  WHERE table_name = '{table}'
  GROUP BY table_catalog, table_schema, table_name
)
SELECT
  '{dataset}' AS dataset,
  '{table}' AS table,
  table_id,
  last_modified_utc,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified_utc, HOUR) AS hours_diff,
  FALSE AS raise_flag  
FROM meta
