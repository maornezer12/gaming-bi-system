/*
Initialize FCT SESSIONS
run_time
{run_time}
Session start detection:
- Using MIN(time) per session_id for session start timestamp.
*/

CREATE OR REPLACE TABLE `{project}.{dataset_dst}.{table_dst}`
PARTITION BY dt
CLUSTER BY user_id
OPTIONS (description = "Curated session-level fact") AS
SELECT
  DATE(MIN(time)) AS dt,
  ANY_VALUE(user_id) AS user_id,
  session_id,
  MIN(time) AS session_start_ts,
  COALESCE(
    ANY_VALUE(session_length_seconds),
    CAST(ANY_VALUE(session_time) AS INT64)
  ) AS session_length_seconds,
  ANY_VALUE(device_type) AS device_type,
  COALESCE(ANY_VALUE(country), ANY_VALUE(store_country)) AS country
FROM `{project}.{dataset_src}.{table_src}`
WHERE user_id IS NOT NULL
  AND session_id IS NOT NULL
GROUP BY session_id;