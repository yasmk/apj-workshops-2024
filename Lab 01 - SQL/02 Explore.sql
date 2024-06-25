-- Returns all the tables.
SHOW TABLES IN catalog.database;


-- Select all the records from the table.
SELECT * FROM catalog.database.fact_apj_sales;


-- Return information about schema, partitioning, table size, and so on.
DESCRIBE DETAIL catalog.database.fact_apj_sales;


-- Returns provenance information, including the operation, 
-- user, and so on, for each write to a table.
DESCRIBE HISTORY catalog.database.fact_apj_sales;


-- Display detailed information about the specified columns, 
-- including the column statistics collected by the command, and additional metadata information.
DESCRIBE EXTENDED catalog.database.fact_apj_sales;


-- Insert new records into the table.
INSERT INTO
  catalog.database.fact_apj_sales (customer_skey, slocation_skey, sale_id, ts, order_source, order_state, unique_customer_id,store_id)
VALUES
  ("3157", "7", "00009e08-3343-4a88-b40d-a66fede2cdff", now(), "IN-STORE", "COMPLETED", "SYD01-15", "SYD01"),
  ("3523", "5", "00041cc6-30f1-433d-97b5-b92191a92efb", now(), "ONLINE", "COMPLETED", "SYD01-48", "SYD01");


-- Return the history information showing the new inserts.
DESCRIBE HISTORY catalog.database.fact_apj_sales;


-- Return the number of records in version 1 of the table.
SELECT COUNT(*) FROM catalog.database.fact_apj_sales VERSION AS OF 1;


-- Return the number of records in version 2 of the table.
SELECT COUNT(*) FROM catalog.database.fact_apj_sales VERSION AS OF 2;


-- Restores a Delta table to an earlier state.
RESTORE TABLE catalog.database.fact_apj_sales TO VERSION AS OF 1;


-- Return history information showing the restore.
DESCRIBE HISTORY catalog.database.fact_apj_sales;


-- Drop table.
DROP TABLE catalog.database.store_data_json;

-- Show dropped tables.
SHOW TABLES DROPPED in catalog.database;

-- Undrop table.
UNDROP TABLE catalog.database.store_data_json;


-- Select all the records from the table.
SELECT * FROM catalog.database.fact_apj_sales;