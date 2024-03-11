-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Validate Dataset
-- MAGIC
-- MAGIC Explore/Validate the dataset from the previous notebook, which will be used to build the gold table.
-- MAGIC
-- MAGIC Relpace `<my_schema>` with the schema that was created for you in the setup script; ie `odl_user_xxxxxxx`

-- COMMAND ----------

SELECT * FROM apjworkshop24.<my_schema>.top_orders_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ask Databricks Assistant to help with creating a Gold Table
-- MAGIC
-- MAGIC 1. Create a new Notebook and set the default language to SQL
-- MAGIC 2. You can ask the assistant to help you build a query to **aggregate the total sales by store and date**, by leveraging the inline assistant within the notebook cell. Or you can use the query below. Make sure you update the catalog and schema name accordingly.
-- MAGIC
-- MAGIC ```
-- MAGIC select  
-- MAGIC     date(ts) as order_date,
-- MAGIC     store_id,
-- MAGIC     sum(order_total) total_sales_by_store
-- MAGIC from 
-- MAGIC     apjworkshop24.<my_schema>.top_orders_silver
-- MAGIC group by all
-- MAGIC ```
-- MAGIC
-- MAGIC 3. Edit the query and create or replace a Gold table to persist the results in the Lakehouse as a Delta Table. Name the new table `top_stores_gold`. Make sure you create it in the correct catalog and schema.
-- MAGIC

-- COMMAND ----------

-- Prompt AI Assistant here

-- COMMAND ----------

CREATE OR REPLACE TABLE
apjworkshop24.<my_schema>.top_stores_gold
AS
-- Copy select statement here
