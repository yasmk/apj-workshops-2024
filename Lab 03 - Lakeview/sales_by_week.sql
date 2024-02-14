select
  sales.ts::timestamp as date,
  items.product_cost as cost,
  items.product_id as product,
  items.product_size,
  sales.sale_id
from
  apjworkshop24.{username}.apj_sales_fact sales
  join apjworkshop24.{username}.apj_sale_items_fact items 
       on items.sale_id = sales.sale_id