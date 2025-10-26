/*
 Initialize PANEL table
 run_time
 {run_time}
 */

CREATE OR REPLACE TABLE `{project}.{dataset_dst}.{table_dst}`
(
  dt                    DATE,
  user_id               STRING,
--   Version               STRING,
--   Platform              STRING,
  Level                 INTEGER,
  t_Session_Start       INTEGER,
  t_Match_Start         INTEGER,
  t_revenue             FLOAT64,
  t_coins_gained        INTEGER,
--   t_XpEarned            INTEGER,
--   last_activity_dt      DATE
)
PARTITION BY {partition_att}
OPTIONS (description = "{description}")
AS
SELECT
  dt,
  user_id,
--   MAX(Version) AS version,
--   MAX(Platform) AS platform,
  MAX(player_rank) AS level,
  SUM(CASE WHEN event_name = 'event_start_time' THEN 1 END) AS t_Session_Start,
  SUM(CASE WHEN event_name = 'Match_Start' THEN 1 END) AS t_Match_Start,
  SUM(price) AS t_revenue,
  SUM(coins_gained) AS t_coins_gained
FROM `{project}.{dataset_src}.{table_src}`
WHERE {partition_att} <= DATE("{date}")
GROUP BY ALL;
