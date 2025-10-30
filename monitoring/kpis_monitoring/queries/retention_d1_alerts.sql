/*
Run_time
{run_time}

KPIs Name
{kpi_name}

Description
{description}
*/

-- D1 retention: share of install cohort active on cohort_dt + 1
WITH installs AS (
  SELECT user_id, install_dt AS cohort_dt
  FROM `{project}.{dataset}.dim_user`
  WHERE install_dt IS NOT NULL
  ORDER BY cohort_dt DESC
  LIMIT 5
),
activity AS (
  SELECT DISTINCT user_id, dt
  FROM `{project}.{dataset}.fct_sessions`
)
SELECT
  SAFE_DIVIDE(ABS(ret_d1 - {d1}), NULLIF({d1}, 0)) > {thresh_in_percent} AS raise_flag,
  "{kpi_name}" AS kpi,
  cohort_dt AS date,
  ret_d1 AS metric,
  {d1} AS previous_metric,
  "{project}.{dataset}.dim_user" AS table_name
FROM (
  SELECT
    i.cohort_dt,
    SAFE_DIVIDE(
      COUNTIF(a.dt = DATE_ADD(i.cohort_dt, INTERVAL 1 DAY)),
      NULLIF(COUNT(*), 0)
    ) AS ret_d1,
    LAG(
      SAFE_DIVIDE(
        COUNTIF(a.dt = DATE_ADD(i.cohort_dt, INTERVAL 1 DAY)),
        NULLIF(COUNT(*), 0)
      ), 1
    ) OVER (ORDER BY i.cohort_dt) AS {d1}
  FROM installs i
  LEFT JOIN activity a USING(user_id)
  GROUP BY i.cohort_dt
)
ORDER BY date DESC
LIMIT 1;
