/*
Run_time
{run_time}

KPIs Name
{kpi_name}

Description
{description}
*/

-- ARPDAU computed as revenue / DAU directly from curated facts
WITH dau AS (
  SELECT dt, COUNT(DISTINCT user_id) AS dau
  FROM `{project}.{dataset}.fct_sessions`
  GROUP BY dt
  ORDER BY dt DESC
  LIMIT 5
),
rev AS (
  SELECT dt, SUM(price) AS revenue
  FROM `{project}.{dataset}.fct_purchases`
  GROUP BY dt
  ORDER BY dt DESC
  LIMIT 5
),
joined AS (
  SELECT
    d.dt,
    SAFE_DIVIDE(r.revenue, NULLIF(d.dau, 0)) AS arpdau
  FROM dau d
  LEFT JOIN rev r USING(dt)
)
SELECT
  SAFE_DIVIDE(ABS(arpdau - {d1}), NULLIF({d1}, 0)) > {thresh_in_percent} AS raise_flag,
  "{kpi_name}" AS kpi,
  dt AS date,
  arpdau AS metric,
  {d1} AS previous_metric,
  "{project}.{dataset}.fct_purchases" AS table_name
FROM (
  SELECT
    dt,
    arpdau,
    LAG(arpdau, 1) OVER (ORDER BY dt) AS {d1}
  FROM joined
)
ORDER BY date DESC
LIMIT 1;
