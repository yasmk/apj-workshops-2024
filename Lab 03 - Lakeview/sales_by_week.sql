select
  sales.ts::timestamp as date,
  items.product_cost::double as cost,
  items.product_id as product,
  items.product_size,
  sales.sale_id,
  sales.store_id
from
  apjworkshop24.{username}.fact_apj_sales sales
  join apjworkshop24.{username}.fact_apj_sale_items items 
       on items.sale_id = sales.sale_id