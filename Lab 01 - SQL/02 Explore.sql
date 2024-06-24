-- Databricks notebook source
-- DBTITLE 1,Returns all the tables
SHOW TABLES IN catalog.database;

-- COMMAND ----------

-- DBTITLE 1,Select all the records from the table
SELECT * FROM catalog.database.fact_apj_sales;

-- COMMAND ----------

-- DBTITLE 1,Return information about schema, partitioning, table size, and so on
DESCRIBE DETAIL catalog.database.fact_apj_sales;

-- COMMAND ----------

-- DBTITLE 1,Database Sales History
DESCRIBE HISTORY catalog.database.fact_apj_sales;

-- COMMAND ----------

-- DBTITLE 1,Display detailed information about the specified columns
DESCRIBE EXTENDED catalog.database.fact_apj_sales;

-- COMMAND ----------

-- DBTITLE 1,Insert new records into the table
INSERT INTO
  catalog.database.fact_apj_sales (customer_skey, slocation_skey, sale_id, ts, order_source, order_state, unique_customer_id,store_id)
VALUES
  ("3157", "7", "00009e08-3343-4a88-b40d-a66fede2cdff", current_timestamp(), "IN-STORE", "COMPLETED", "SYD01-15", "SYD01"),
  ("3523", "5", "00041cc6-30f1-433d-97b5-b92191a92efb", current_timestamp(), "ONLINE", "COMPLETED", "SYD01-48", "SYD01");

-- COMMAND ----------

-- DBTITLE 1,Return the history information showing the new inserts
DESCRIBE HISTORY catalog.database.fact_apj_sales;

-- COMMAND ----------

-- DBTITLE 1,Return the number of records in version 1 of the table
SELECT COUNT(*) FROM catalog.database.fact_apj_sales VERSION AS OF 1;

-- COMMAND ----------

-- DBTITLE 1,Return the number of records in version 2 of the table
SELECT COUNT(*) FROM catalog.database.fact_apj_sales VERSION AS OF 2;

-- COMMAND ----------

-- DBTITLE 1,Restores a Delta table to an earlier state
RESTORE TABLE catalog.database.fact_apj_sales TO VERSION AS OF 1;

-- COMMAND ----------

-- DBTITLE 1,Return history information showing the restore
DESCRIBE HISTORY catalog.database.fact_apj_sales;

-- COMMAND ----------

-- DBTITLE 1,Drop table
DROP TABLE catalog.database.store_data_json;

-- COMMAND ----------

-- DBTITLE 1,Show dropped tables
SHOW TABLES DROPPED in catalog.database;

-- COMMAND ----------

-- DBTITLE 1,Undrop table
UNDROP TABLE catalog.database.store_data_json;
