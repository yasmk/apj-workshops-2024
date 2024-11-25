-- Select all data from the table
SELECT * FROM catalog.database.store_data_json;


-- Extract a top-level column
SELECT raw:owner FROM catalog.database.store_data_json;


-- Extract nested fields
SELECT raw:store.bicycle FROM catalog.database.store_data_json;


-- Escape characters
SELECT raw:owner, raw:`fb:testid`, raw:`zip code` FROM catalog.database.store_data_json;


-- Extract values from arrays
SELECT raw:store.fruit[0], raw:store.fruit[1] FROM catalog.database.store_data_json;


-- Flatten variant objects and arrays
SELECT
  key, value 
FROM 
  catalog.database.store_data_json, 
  LATERAL variant_explode(catalog.database.store_data_json.raw:store);