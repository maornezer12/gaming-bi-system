/*
 Run_time
 {run_time}

 KPIs Name
 {kpi_name}

 Description
 {description}
*/

-- Last activity = active users per day from curated sessions
SELECT
  SAFE_DIVIDE(ABS(active_users - {d1}), NULLIF({d1}, 0)) > {thresh_in_percent} AS raise_flag,
  "{kpi_name}" AS kpi,
  dt AS last_activity,
  active_users AS metric,
  {d1} AS previous_metric,
  "{project}.{dataset}.{table_id}" AS table_name
FROM (
  SELECT
    dt,
    COUNT(DISTINCT user_id) AS active_users,
    LAG(COUNT(DISTINCT user_id), 1) OVER (ORDER BY dt) AS {d1}
  FROM `{project}.{dataset}.{table_id}`
  GROUP BY dt
  ORDER BY dt DESC
  LIMIT 5
)
ORDER BY last_activity DESC
LIMIT 1;
