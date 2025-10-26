/*
 Initialize USER PANEL table
 run_time
 {run_time}
 */

CREATE OR REPLACE TABLE`{project}.{dataset_dst}.{table_dst}`
(
  user_id           STRING  OPTIONS(description='User unique ID'),
  install_dt            DATE    OPTIONS(description='First activity date'),
--   Current_Version       STRING,
--   Current_Platform      STRING,
  Level                 INTEGER,
  t_active_days         INTEGER,
  t_Session_Start       INTEGER,
  t_Match_Start         INTEGER,
  t_revenue             FLOAT64,
  t_coins_gained        INTEGER,
--   t_XpEarned            INTEGER,
  last_activity_dt      DATE    OPTIONS(description='Last activity date')
)
PARTITION BY {partition_att}
OPTIONS (description = "{description}")
AS
SELECT
  user_id,
  MIN(dt) as install_dt,
--   substring(max(concat(cast(dt as string), Version)), 11) AS version,
--   substring(max(concat(cast(dt as string), Platform)), 11) AS platform
  MAX(level) AS level,
  COUNT(1) AS t_active_days,
  SUM(t_Session_Start) AS t_Session_Start,
  SUM(t_Match_Start) AS t_Match_Start,
  SUM(t_revenue) AS t_revenue,
  SUM(t_coins_gained) AS t_coins_gained,
--   SUM(t_XpEarned) AS t_XpEarned,
  MAX(dt) as last_activity_dt
FROM `{project}.{dataset_src}.{table_src}`
WHERE {partition_att} <= DATE("{date}")
GROUP BY ALL;
