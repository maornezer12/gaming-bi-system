/*
Load FCT SESSIONS (daily)
run_time
{run_time}
*/

MERGE `{project}.{dataset_dst}.{table_dst}` T
USING (
  SELECT
    DATE(MIN(COALESCE(event_start_time, time))) AS dt,
    ANY_VALUE(user_id) AS user_id,
    session_id,
    MIN(COALESCE(event_start_time, time)) AS session_start_ts,
    COALESCE(
      ANY_VALUE(session_length_seconds),
      CAST(ANY_VALUE(session_time) AS INT64)
    ) AS session_length_seconds,
    ANY_VALUE(device_type) AS device_type,
    COALESCE(ANY_VALUE(country), ANY_VALUE(store_country)) AS country
  FROM `{project}.{dataset_src}.{table_src}`
  WHERE DATE(COALESCE(event_start_time, time)) = DATE("{date}")
    AND user_id IS NOT NULL
    AND session_id IS NOT NULL
  GROUP BY session_id
) S
ON T.session_id = S.session_id
WHEN NOT MATCHED THEN
  INSERT (dt, user_id, session_id, session_start_ts, session_length_seconds, device_type, country)
  VALUES (S.dt, S.user_id, S.session_id, S.session_start_ts, S.session_length_seconds, S.device_type, S.country);
