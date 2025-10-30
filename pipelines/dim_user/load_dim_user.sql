/*
Load DIM USER (daily)
run_time
{run_time}
*/

-- expects {date} as processing date, provided by runner

MERGE `{project}.{dataset_dst}.{table_dst}` T
USING (
  WITH c AS (
    SELECT *
    FROM `{project}.{dataset_src}.{table_src}`
    WHERE DATE(time) = DATE("{date}") AND user_id IS NOT NULL
  ),
  firsts AS (
    SELECT
      user_id,
      COALESCE(
        LEAST(MIN(install_date), DATE(MIN(time))),
        DATE(MIN(time))
      ) AS install_dt_candidate,
      ANY_VALUE(country) AS country,
      ANY_VALUE(store_country) AS store_country,
      ANY_VALUE(device_type) AS device_type,
      MIN(time) AS first_time
    FROM c
    GROUP BY user_id
  )
  SELECT
    user_id,
    COALESCE(install_dt_candidate, DATE(first_time)) AS install_dt,
    COALESCE(country, store_country) AS country_at_install,
    device_type AS device_type_at_install
  FROM firsts
) S
ON T.user_id = S.user_id
WHEN NOT MATCHED THEN
  INSERT (user_id, install_dt, country_at_install, device_type_at_install)
  VALUES (S.user_id, S.install_dt, S.country_at_install, S.device_type_at_install);
