/*
 Initialize USER PANEL table
 run_time
 {run_time}
 */

CREATE OR REPLACE TABLE`{project}.{dataset_dst}.{table_dst}`
PARTITION BY {partition_att}
OPTIONS (description = "{description}")
AS
SELECT
  user_id,
  MIN(dt) as install_dt,
--   substring(max(concat(cast(dt as string), Version)), 11) AS version,
--   substring(max(concat(cast(dt as string), Platform)), 11) AS platform
  MAX(level) AS level,
  COUNT(DISTINCT dt) AS t_active_days,
  SUM(t_Session_Start) AS t_Session_Start,
  SUM(t_Match_Start) AS t_Match_Start,
  SUM(t_revenue) AS t_revenue,
  SUM(t_coins_gained) AS t_coins_gained,
--   SUM(t_XpEarned) AS t_XpEarned,
  MAX(dt) as last_activity_dt
FROM `{project}.{dataset_src}.{table_src}`
WHERE dt <= DATE("{date}")
GROUP BY user_id;
