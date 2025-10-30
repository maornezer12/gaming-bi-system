/*
Load FCT PURCHASES (daily)
run_time
{run_time}
*/

MERGE `{project}.{dataset_dst}.{table_dst}` T
USING (
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
  WHERE DATE(time) = DATE("{date}")
    AND user_id IS NOT NULL
    AND (
      event_name = 'purchase' OR
      SAFE_CAST(price AS FLOAT64) > 0 OR
      transaction_id IS NOT NULL
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(transaction_id, CONCAT(user_id,'|',CAST(time AS STRING),'|',COALESCE(product_id,'')))
    ORDER BY time ASC
  ) = 1
) S
ON COALESCE(T.transaction_id, CONCAT(T.user_id,'|',CAST(T.ts AS STRING),'|',COALESCE(T.product_id,'')))
   = COALESCE(S.transaction_id, CONCAT(S.user_id,'|',CAST(S.ts AS STRING),'|',COALESCE(S.product_id,'')))
WHEN NOT MATCHED THEN
  INSERT (dt, user_id, ts, transaction_id, product_id, product_name, price, currency, is_first_purchase, payment_provider)
  VALUES (S.dt, S.user_id, S.ts, S.transaction_id, S.product_id, S.product_name, S.price, S.currency, S.is_first_purchase, S.payment_provider);
