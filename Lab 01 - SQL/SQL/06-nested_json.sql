-- Set your catalog and database.
USE catalog.database;


-- Select all data from the table
SELECT * FROM store_data_json;


-- Extract a top-level column
SELECT raw:owner FROM store_data_json;


-- Extract nested fields
SELECT raw:store.bicycle FROM store_data_json;


-- Escape characters
SELECT raw:owner, raw:`fb:testid`, raw:`zip code`  FROM store_data_json;


-- Extract values from arrays
SELECT raw:store.fruit[0], raw:store.fruit[1] FROM store_data_json;


-- Extract subfields from arrays
SELECT raw:store.book[*].isbn FROM store_data_json;

