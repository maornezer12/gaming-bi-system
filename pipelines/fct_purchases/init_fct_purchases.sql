/*
Initialize FCT PURCHASES
run_time
{run_time}
*/

CREATE OR REPLACE TABLE `{project}.{dataset_dst}.{table_dst}`
PARTITION BY dt
CLUSTER BY user_id
OPTIONS (description = "Curated purchase-level fact") AS
SELECT
  DATE(time) AS dt,
  user_id,
  time AS ts,
  transaction_id,
  product_id,
  product_name,
  price,
  currency,
  is_first_purchase,
  payment_provider
FROM `{project}.{dataset_src}.{table_src}`
WHERE user_id IS NOT NULL
  AND (
    event_name = 'purchase' OR
    SAFE_CAST(price AS FLOAT64) > 0 OR
    transaction_id IS NOT NULL
  )
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY COALESCE(transaction_id, CONCAT(user_id,'|',CAST(time AS STRING),'|',COALESCE(product_id,'')))
  ORDER BY time ASC
) = 1;
