Databricks notebook source
-- DBTITLE 1,Create a query called "city_list" from the following
SELECT DISTINCT city FROM catalog.database.dim_locations;

-- COMMAND ----------

-- DBTITLE 1,Get sales by store and city
SELECT
  locations.name AS store,
  locations.city AS city,
  count(*) AS sales
FROM
  catalog.database.fact_apj_sales sales
  JOIN catalog.database.fact_apj_sale_items items
    on items.sale_id = sales.sale_id
  JOIN catalog.database.dim_locations locations
    on sales.store_id = locations.id
GROUP BY
  ALL;

-- COMMAND ----------

-- DBTITLE 1,Create a filter for city
SELECT
  locations.name AS store,
  locations.city AS city,
  count(*) AS sales
FROM
  catalog.database.fact_apj_sales sales
  JOIN catalog.database.fact_apj_sale_items items
    ON items.sale_id = sales.sale_id
  JOIN catalog.database.dim_locations locations
    ON sales.store_id = locations.id
WHERE
  city IN ({{city_list}})
GROUP BY
  ALL;