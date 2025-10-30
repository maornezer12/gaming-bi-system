/*
 Run_time
 {run_time}

 KPIs Name
 {kpi_name}

 Description
 {description}
*/

-- Installs computed from curated dim_user by install_dt
SELECT
  SAFE_DIVIDE(ABS(installs - {d1}), NULLIF({d1}, 0)) > {thresh_in_percent} AS raise_flag,
  "{kpi_name}" AS kpi,
  install_dt AS install_date,
  installs AS metric,
  {d1} AS previous_metric,
  "{project}.{dataset}.{table_id}" AS table_name
FROM (
  SELECT
    install_dt,
    COUNT(*) AS installs,
    LAG(COUNT(*), 1) OVER (ORDER BY install_dt) AS {d1}
  FROM `{project}.{dataset}.{table_id}`
  GROUP BY install_dt
  ORDER BY install_dt DESC
  LIMIT 5
)
ORDER BY install_date DESC
LIMIT 1;
