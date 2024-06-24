/*
 * Row Filters
 */

-- Create a function to filter on stores.
-- The function evaluates if the user is part of the 'admins' group,
-- if they are, it returns all rows, if they are not, it executes the filter.
CREATE OR REPLACE FUNCTION
  catalog.database.mel_stores_filter(store_id STRING) 
RETURN
  CASE WHEN is_member('admins') THEN true ELSE store_id like 'MEL0%' END;


-- Add filter to table.
ALTER TABLE
  catalog.database.dim_customer
SET
  ROW FILTER catalog.database.mel_stores_filter ON (store_id);


-- Select from table. You should see only rows within the Melbourne stores.
SELECT * FROM catalog.database.dim_customer;


-- Remove filter.
ALTER TABLE
  catalog.database.dim_customer
DROP
  ROW FILTER;


/*
 * Column Masks
 */

-- Create a function to mask names.
-- The function evaluates if the user is part of the 'admins' group,
-- if they are, it returns all values, if they are not, it executes the mask.
CREATE OR REPLACE FUNCTION
  catalog.database.name_mask(name STRING)
RETURN
  CASE WHEN is_member('admins') THEN name ELSE '***** *****' END;

CREATE OR REPLACE FUNCTION
  catalog.database.email_mask(email STRING)
RETURN
  CASE WHEN is_member('admins') THEN email ELSE regexp_replace(email, '(.*)@', '') END;


-- Add masks to table.
ALTER TABLE
  catalog.database.dim_customer
ALTER COLUMN 
  name
SET MASK
  catalog.database.name_mask;

ALTER TABLE
  catalog.database.dim_customer
ALTER COLUMN 
  email
SET MASK
  catalog.database.email_mask;


-- Select from table. All names should be masked.
SELECT * FROM catalog.database.dim_customer;