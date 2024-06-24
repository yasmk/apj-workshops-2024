/*
 * Databricks SQL alerts periodically run queries, evaluate defined conditions, 
 * and send notifications if a condition is met. You can set up alerts to monitor 
 * your business and send notifications when reported data falls outside 
 * of expected limits. Scheduling an alert executes its underlying query and 
 * checks the alert criteria. This is independent of any schedule that might 
 * exist on the underlying query.
 */

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