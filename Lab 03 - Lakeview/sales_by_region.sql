select
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost as cost,
  locations.city
from
  apjworkshop24.{username}.fact_apj_sales sales
  join apjworkshop24.{username}.fact_apj_sale_items items 
       on items.sale_id = sales.sale_id
  join apjworkshop24.{username}.dim_locations locations
       on sales.store_id = locations.id