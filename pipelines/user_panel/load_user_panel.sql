/*
 Load daily incremental data into User Panel table
 run_time: {run_time}
*/

-- Update existing users
UPDATE `{project}.{dataset_dst}.{table_dst}` AS dst
SET
--     Current_Version   = src.Current_Version,
--     Current_Platform  = src.Current_Platform,
    Level             = src.Level,
    t_active_days     = dst.t_active_days + 1,
    t_Session_Start     = dst.t_Session_Start + IFNULL(src.t_Session_Start, 0),
    t_Match_Start     = dst.t_Match_Start + IFNULL(src.t_Match_Start, 0),
    t_revenue         = dst.t_revenue + IFNULL(src.t_revenue, 0),
    t_coins_gained      = dst.t_coins_gained + IFNULL(src.t_coins_gained, 0),
--     t_XpEarned        = dst.t_XpEarned + IFNULL(src.t_XpEarned, 0),
    last_activity_dt  = src.dt
FROM `{project}.{dataset_src}.{table_src}` AS src
WHERE dst.user_id = src.user_id
  AND {partition_att} = DATE("{date}");

-- Insert new users (first-time activity)
INSERT INTO `{project}.{dataset_dst}.{table_dst}`
SELECT
    daily.user_id,
    daily.dt AS install_dt,
--     daily.Version AS Current_Version,
--     daily.Platform AS Current_Platform,
    daily.Level,
    1 AS t_active_days,
    daily.t_Session_Start,
    daily.t_Match_Start,
    daily.t_revenue,
    daily.t_coins_gained,
--     daily.t_XpEarned,
    daily.dt AS last_activity_dt
FROM `{project}.{dataset_src}.{table_src}` AS daily
LEFT JOIN `{project}.{dataset_dst}.{table_dst}` AS user
USING (user_id)
WHERE {partition_att} = DATE("{date}")
  AND user.user_id IS NULL;

-- Validation
/*
SELECT
  {partition_att},
  COUNT(1)
FROM `{project}.{dataset_dst}.{table_dst}`
WHERE {partition_att} = DATE("{date}")
GROUP BY 1 ORDER BY 1 DESC;
*/
