SELECT
  sales.ts :: timestamp as date,
  items.product_cost :: double as cost,
  items.product_id as product,
  items.product_size,
  sales.sale_id,
  sales.store_id
FROM
  catalog.database.fact_apj_sales sales
  JOIN catalog.database.fact_apj_sale_items items ON items.sale_id = sales.sale_id