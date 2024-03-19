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