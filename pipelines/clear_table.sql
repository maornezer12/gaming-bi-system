/*
 Delete data from {etl_name} table by date
 run_time: {run_time}
*/

DELETE FROM `{project}.{dataset_dst}.{table_dst}`
WHERE {partition_att} = DATE("{date}");


/*
 Validation:

SELECT
 {partition_att},
 COUNT(1),
FROM `{project}.{dataset_dst}.{table_dst}`
GROUP BY 1 ORDER BY 1 DESC
 */