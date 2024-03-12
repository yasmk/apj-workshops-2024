--------------------
/* replace your_database_name in the following command by your database name 
get your databse name by running the first cell of the prep-notebook 
*/
--------------------
USE <<catalog>>.<<databasename>>;;
--------------------

-- FOR FILTERING
select
  locations.city,
  count(*) as cnt
from
 fact_apj_sales sales
  join fact_apj_sale_items items 
       on items.sale_id = sales.sale_id
  join dim_locations locations
       on sales.store_id = locations.id
group by locations.city;


-- FOR PARAMETERS
select
  locations.city,
  count(*) as cnt
from
 fact_apj_sales sales
  join fact_apj_sale_items items 
       on items.sale_id = sales.sale_id
  join dim_locations locations
       on sales.store_id = locations.id
group by locations.city
HAVING cnt > 100000 -- replace this with a parameter
