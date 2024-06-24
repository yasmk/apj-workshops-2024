-- Select all records from the fact table and join it
-- with the 'items' and 'locations' dimensions
SELECT
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost::double as cost,
  locations.city
FROM
 catalog.database.fact_apj_sales sales
  JOIN catalog.database.fact_apj_sale_items items 
       ON items.sale_id = sales.sale_id
  JOIN catalog.database.dim_locations locations
       ON sales.store_id = locations.id;


-- Create a reusable view of the above Select statement
CREATE VIEW IF NOT EXISTS catalog.database.vw_sales_cost_location AS
SELECT
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost::double as cost,
  locations.city
FROM
 catalog.database.fact_apj_sales sales
  JOIN catalog.database.fact_apj_sale_items items 
       ON items.sale_id = sales.sale_id
  JOIN catalog.database.dim_locations locations
       ON sales.store_id = locations.id;
  

-- Select all reocrds from the view
SELECT * FROM catalog.database.vw_sales_cost_location;


-- Describe the view
DESCRIBE EXTENDED catalog.database.vw_sales_cost_location;

/*
 * In Databricks SQL, materialized views are Unity Catalog managed 
 * tables that allow users to precompute results based on the latest 
 * version of data in source tables. Materialized views on Databricks 
 * differ from other implementations as the results returned reflect 
 * the state of data when the materialized view was last refreshed 
 * rather than always updating results when the materialized view is queried. 
 * You can manually refresh materialized views or schedule refreshes.
 */


-- Create a materialized view of the above Select statement
CREATE MATERIALIZED VIEW IF NOT EXISTS catalog.database.mv_sales_cost_location AS
SELECT
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost::double as cost,
  locations.city
FROM
 catalog.database.fact_apj_sales sales
  JOIN catalog.database.fact_apj_sale_items items 
       ON items.sale_id = sales.sale_id
  JOIN catalog.database.dim_locations locations
       ON sales.store_id = locations.id;
  

-- Select all reocrds from the view
SELECT * FROM catalog.database.mv_sales_cost_location;


-- Describe the materialized view
DESCRIBE EXTENDED catalog.database.mv_sales_cost_location;