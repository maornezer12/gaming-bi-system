/*
Initialize DIM USER
run_time
{run_time}
*/

CREATE OR REPLACE TABLE `{project}.{dataset_dst}.{table_dst}`
OPTIONS (description = "Curated dimension of first-seen users") AS
WITH firsts AS (
  SELECT
    user_id,
    COALESCE(
      LEAST(MIN(install_date) OVER (PARTITION BY user_id), DATE(MIN(time) OVER (PARTITION BY user_id))),
      DATE(MIN(time) OVER (PARTITION BY user_id))
    ) AS install_dt_candidate,
    country,
    store_country,
    device_type,
    time,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY time ASC) AS rn
  FROM `{project}.{dataset_src}.{table_src}`
  WHERE user_id IS NOT NULL
)
SELECT
  user_id,
  COALESCE(install_dt_candidate, DATE(time)) AS install_dt,
  COALESCE(country, store_country) AS country_at_install,
  device_type AS device_type_at_install
FROM firsts
WHERE rn = 1;
