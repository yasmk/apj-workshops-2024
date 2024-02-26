--------------------
/* replace your_database_name in the following command by your database name 
get your databse name by running the first cell of the prep-notebook 
*/
--------------------
USE <<catalog>>.<<databasename>>;;
--------------------

select
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost as cost,
  locations.city
from
 fact_apj_sales sales
  join fact_apj_sale_items items 
       on items.sale_id = sales.sale_id
  join dim_locations locations
       on sales.store_id = locations.id;


-- CREATE a VIEW

CREATE VIEW IF NOT EXISTS vw_sales_cost_location
AS
select
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost as cost,
  locations.city
from
 fact_apj_sales sales
  join fact_apj_sale_items items 
       on items.sale_id = sales.sale_id
  join dim_locations locations
       on sales.store_id = locations.id
;
  
SELECT * FROM vw_sales_cost_location

