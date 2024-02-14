select
  sales.ts::timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost as cost,
  locations.city
from
  apjworkshop24.{username}.apj_sales_fact sales
  join apjworkshop24.{username}.apj_sale_items_fact items 
       on items.sale_id = sales.sale_id
  join apjworkshop24.{username}.dim_locations locations
       on sales.store_id = locations.id