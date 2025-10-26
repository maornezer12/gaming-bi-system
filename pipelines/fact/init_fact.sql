/*
 Initialize FACT table
 run_time
 {run_time}
 */
CREATE OR REPLACE TABLE `{project}.{dataset_dst}.{table_dst}`
PARTITION BY {partition_att}
options (description = "{description}")
AS
SELECT  *
FROM `ppltx-ba-course.{dataset_src}.{table_src}`
WHERE
  {partition_att} <= DATE("{date}")
--   limit 1000
