/*
 Load daily incremental data into PANEL table
 run_time: {run_time}
*/

INSERT INTO `{project}.{dataset_dst}.{table_dst}`
SELECT
  dt,
  user_id,
--   MAX(Version) AS version,
--   MAX(Platform) AS platform,
  MAX(player_rank) AS level,
  SUM(CASE WHEN event_name = 'event_start_time' THEN 1 END) AS t_Session_Start,
  SUM(CASE WHEN event_name = 'Match_Start' THEN 1 END) AS t_Match_Start,
  SUM(price) AS t_revenue,
  SUM(coins_gained) AS t_coins_gained,
FROM `{project}.{dataset_src}.{table_src}`
WHERE {partition_att} = DATE("{date}")
GROUP BY ALL;


/*
 Validation:

SELECT
 {partition_att},
 COUNT(1),
FROM `{project}.{dataset_dst}.{table_dst}`
WHERE {partition_att} = DATE("{date}")
GROUP BY 1 ORDER BY 1 DESC
 */