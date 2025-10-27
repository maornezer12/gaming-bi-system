/*
Run time
{run_time}
{description}
etl_runner.py
*/

-- Pick the last run of a given job from logs.daily_logs and decide if it's stale.
SELECT
  DATETIME_DIFF(CURRENT_DATETIME(), ts, HOUR) > {thresh_in_hours} AS raise_flag,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', ts)                          AS last_ts,
  job_name,
  job_type,
  step_name,
  file_name,
  ts,
  dt,
  uid,
  username,
  message
FROM `{project}.logs.daily_logs`
WHERE TRUE
  AND job_name = '{job_name}'
  AND step_name = '{step_name}'
ORDER BY ts DESC
LIMIT 1;
