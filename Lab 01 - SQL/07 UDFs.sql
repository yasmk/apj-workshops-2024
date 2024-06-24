/*
 * A user-defined function (UDF) is a function defined by a user, 
 * allowing custom logic to be reused in the user environment. 
 * Databricks has support for many different types of UDFs to allow 
 * for distributing extensible logic. This article introduces some of 
 * the general strengths and limitations of UDFs.
 */

-- Create a UDF that returns a literal.
CREATE OR REPLACE FUNCTION catalog.database.blue()
  RETURN '0000FF';
  
SELECT catalog.database.blue();


-- Create a UDF that encapsulates other functions.
CREATE OR REPLACE FUNCTION catalog.database.to_hex(x INT)
  RETURN lpad(hex(least(greatest(0, x), 255)), 2, 0);

SELECT catalog.database.to_hex(id) FROM range(2);


-- Create a lookup table.
CREATE OR REPLACE TABLE catalog.database.colors(rgb STRING NOT NULL, name STRING NOT NULL);

INSERT INTO catalog.database.colors VALUES
  ('FF00FF', 'magenta'),
  ('FF0080', 'rose'),
  ('BFFF00', 'lime'),
  ('7DF9FF', 'electric blue');


-- Create function to translate an RGB colour to its name.
CREATE OR REPLACE FUNCTION catalog.database.from_rgb_scalar(rgb STRING)
  RETURN SELECT FIRST(name) FROM catalog.database.colors WHERE colors.rgb = from_rgb_scalar.rgb;


-- Select the names of the colours.
SELECT catalog.database.from_rgb_scalar(rgb) FROM VALUES ('7DF9FF'), ('BFFF00') AS codes(rgb);