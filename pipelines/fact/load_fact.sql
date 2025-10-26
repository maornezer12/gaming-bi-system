/*
 This query inserts raw data into Fact table
 run_time
 {run_time}
 */

INSERT INTO `{project}.{dataset_dst}.{table_dst}`
SELECT *
FROM `ppltx-ba-course.{dataset_src}.{table_src}`
WHERE {partition_att} >= DATE("{date}") AND time < CURRENT_TIMESTAMP();

/*
 Validation:

SELECT
 {partition_att},
 COUNT(1),
FROM `{project}.{dataset_dst}.{table_dst}`
WHERE {partition_att} = DATE("{date}")
GROUP BY 1 ORDER BY 1 DESC
 */