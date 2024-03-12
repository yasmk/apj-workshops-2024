-- Set your catalog and database.
USE catalog.database;

-- Returns all the tables,
SHOW TABLES;


-- Select all the records from the table.
SELECT * FROM fact_apj_sales;


-- Return information about schema, partitioning, table size, and so on.
DESCRIBE DETAIL fact_apj_sales;


-- Returns provenance information, including the operation, 
-- user, and so on, for each write to a table.
DESCRIBE HISTORY fact_apj_sales;


-- Display detailed information about the specified columns, 
-- including the column statistics collected by the command, and additional metadata information
DESCRIBE EXTENDED fact_apj_sales;


-- Insert new records into the table
INSERT INTO
  fact_apj_sales (customer_skey, slocation_skey, sale_id, ts, order_source, order_state, unique_customer_id,store_id)
VALUES
  (
    "3157",
    "7",
    "00009e08-3343-4a88-b40d-a66fede2cdff",
    current_timestamp(),
    "IN-STORE",
    "COMPLETED",
    "SYD01-15",
    "SYD01"
  ),
  (
    "3523",
    "5",
    "00041cc6-30f1-433d-97b5-b92191a92efb",
    current_timestamp(),
    "ONLINE",
    "COMPLETED",
    "SYD01-48",
    "SYD01"
  );


-- Return the number of records in the table
SELECT COUNT(*) FROM fact_apj_sales;


-- Select all the records from the table ordered by the 'ts' column in descending order
SELECT * FROM fact_apj_sales ORDER BY ts DESC;


DESCRIBE HISTORY fact_apj_sales;


-- Restores a Delta table to an earlier state.
RESTORE TABLE fact_apj_sales TO VERSION AS OF 1;


DESCRIBE HISTORY fact_apj_sales;


-- Select all records from the table as of a particular table version
SELECT * FROM fact_apj_sales VERSION AS OF 1 ORDER BY ts DESC;