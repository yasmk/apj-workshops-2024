-- Set your catalog and database.
USE catalog.database;


-- Create a UDF that returns a literal.
CREATE OR REPLACE FUNCTION blue()
  RETURN '0000FF';
  
SELECT blue();


-- Create a UDF that encapsulates other functions.
CREATE OR REPLACE FUNCTION to_hex(x INT )
  RETURN lpad(hex(least(greatest(0, x), 255)), 2, 0);

SELECT to_hex(id) FROM range(2);


-- Create a lookup table.
CREATE OR REPLACE TABLE colors(rgb STRING NOT NULL, name STRING NOT NULL);
INSERT INTO colors VALUES
  ('FF00FF', 'magenta'),
  ('FF0080', 'rose'),
  ('BFFF00', 'lime'),
  ('7DF9FF', 'electric blue');


-- Create function to translate an RGB colour to its name.
CREATE OR REPLACE FUNCTION from_rgb_scalar(rgb STRING)
  RETURN SELECT FIRST(name) FROM colors WHERE colors.rgb = from_rgb_scalar.rgb;


-- Select the names of the colours.
SELECT from_rgb_scalar(rgb) FROM VALUES ('7DF9FF'), ('BFFF00') AS codes(rgb);

