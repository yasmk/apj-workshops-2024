-- Set your catalog and database.
USE catalog.database;


-- Set up the alert using the following logic.
SELECT
  store_id,
  COUNT(*) as cnt
FROM
  fact_apj_sales
WHERE
  order_state = 'CANCELED'
GROUP BY
  store_id;


-- Insert new rows that will trigger the above alert.
INSERT INTO
  fact_apj_sales (customer_skey, slocation_skey, sale_id, ts, order_source, order_state, unique_customer_id, store_id)
VALUES
  (
    "3157",
    "8",
    "00009e08-3343-4a88-b40d-a66fede2cdff",
    current_timestamp(),
    "IN-STORE",
    "CANCELED",
    "SYD01-15",
    "SYD01"
  ),
  (
    "3523",
    "8",
    "00041cc6-30f1-433d-97b5-b92191a92efb",
    current_timestamp(),
    "ONLINE",
    "CANCELED",
    "SYD01-48",
    "SYD01"
  ),
  (
    "3523",
    "8",
    "00041cc6-30f1-433d-97b5-b92191a92efb",
    current_timestamp(),
    "ONLINE",
    "CANCELED",
    "SYD01-48",
    "SYD01"
  ),
  (
    "3664",
    "8",
    "00077e8c-20e3-44b5-8eb4-70d15780187d",
    current_timestamp(),
    "IN-STORE",
    "CANCELED",
    "SYD01-606",
    "SYD01"
  );