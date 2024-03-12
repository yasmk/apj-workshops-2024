-- Set your catalog and database.
USE catalog.database;


-- Select all records from the fact table and join it
-- with the 'items' and 'locations' dimensions
select
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost::double as cost,
  locations.city
from
 fact_apj_sales sales
  join fact_apj_sale_items items 
       on items.sale_id = sales.sale_id
  join dim_locations locations
       on sales.store_id = locations.id;



-- Create a reusable view of the above Select statement
CREATE VIEW IF NOT EXISTS vw_sales_cost_location AS
select
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost::double as cost,
  locations.city
from
 fact_apj_sales sales
  join fact_apj_sale_items items 
       on items.sale_id = sales.sale_id
  join dim_locations locations
       on sales.store_id = locations.id;
  

-- Select all reocrds from the view
SELECT * FROM vw_sales_cost_location

