-- Set up the alert using the following logic.
SELECT
  store_id,
  COUNT(*) as cnt
FROM
  catalog.database.fact_apj_sales
WHERE
  order_state = 'CANCELED'
GROUP BY
  store_id;