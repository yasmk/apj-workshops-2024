SELECT
  sales.ts :: timestamp as date,
  sales.store_id,
  sales.sale_id,
  items.product_cost :: double as cost,
  locations.city
FROM
  catalog.database.fact_apj_sales sales
  JOIN catalog.database.fact_apj_sale_items items ON items.sale_id = sales.sale_id
  JOIN catalog.database.dim_locations locations ON sales.store_id = locations.id;