-- Set your catalog and database.
USE catalog.database;


-- Select all records from the fact table and join it
-- with the 'items' and 'locations' dimensions
SELECT
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost::double as cost,
  locations.city
from
 fact_apj_sales sales
  JOIN fact_apj_sale_items items 
       ON items.sale_id = sales.sale_id
  JOIN dim_locations locations
       ON sales.store_id = locations.id;


-- Create a reusable view of the above Select statement
CREATE VIEW IF NOT EXISTS vw_sales_cost_location AS
SELECT
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost::double as cost,
  locations.city
FROM
 fact_apj_sales sales
  JOIN fact_apj_sale_items items 
       ON items.sale_id = sales.sale_id
  JOIN dim_locations locations
       ON sales.store_id = locations.id;
  

-- Select all reocrds from the view
SELECT * FROM vw_sales_cost_location

