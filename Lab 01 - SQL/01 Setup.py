# Databricks notebook source
# DBTITLE 1,Create database
import os

# Get the current user's ID
current_user_id = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)

# Define catalog and database name based on user ID
catalog = "workshop"
database_name = current_user_id.split("@")[0].replace(".", "_").replace("+", "_")

# Use the specified catalog, needs to be created first
spark.sql(f"USE CATALOG {catalog};")

# Create and use the database for the current user
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name};")
spark.sql(f"USE {database_name}")

print("Database created.")

# COMMAND ----------

# DBTITLE 1,Find and replace
import os


def replace_in_files(directory, old_word, new_word):
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".sql"):
                file_path = os.path.join(root, filename)
                with open(file_path, "r", encoding="utf-8") as file:
                    file_contents = file.read()

                # Replace the target string
                file_contents = file_contents.replace(old_word, new_word)

                # Write the file out again
                with open(file_path, "w", encoding="utf-8") as file:
                    file.write(file_contents)


directory_path = f"/Workspace/Users/{current_user_id}/apj-workshops-2024"
replace_in_files(directory_path, "catalog.database", f"{catalog}.{database_name}")
replace_in_files(directory_path, "email_address", current_user_id)

print("Workshop files updated with catalog and schema information.")

# COMMAND ----------

# DBTITLE 1,Create dim_customer
spark.sql(
    """
      CREATE OR REPLACE TABLE dim_customer (
        unique_id VARCHAR(50) NOT NULL,
        pk INT NOT NULL,
        id INT NOT NULL,
        store_id VARCHAR(50) NOT NULL,
        name VARCHAR(255),
        email VARCHAR(255),
        PRIMARY KEY (unique_id)
      )
    """
)

print("dim_customer created.")

# COMMAND ----------

# DBTITLE 1,Create dim_locations
spark.sql(
    """
      CREATE TABLE dim_locations (
        id VARCHAR(50) NOT NULL,
        pk INT NOT NULL,
        name VARCHAR(255),
        email VARCHAR(255),
        city VARCHAR(100),
        hq VARCHAR(255),
        phone_number VARCHAR(50),
        country_code VARCHAR(10),
        PRIMARY KEY (id)
      )
    """
)

print("dim_locations created.")

# COMMAND ----------

# DBTITLE 1,Create dim_products
spark.sql(
    """
      CREATE TABLE dim_products (
        product_skey INT NOT NULL,
        id VARCHAR(255) NOT NULL,
        ingredients STRING,
        name VARCHAR(255),
        current_record CHAR(1),
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        PRIMARY KEY (id)
      )
    """
)

print("dim_products created.")

# COMMAND ----------

# DBTITLE 1,Create fact_apj_sale_items
spark.sql(
    """
      CREATE TABLE fact_apj_sale_items (
        sale_id VARCHAR(255) NOT NULL,
        product_id VARCHAR(255),
        store_id VARCHAR(50),
        product_size VARCHAR(50),
        product_cost DECIMAL(10, 2),
        product_ingredients STRING,
        product_skey INT,
        slocation_skey INT,
        unique_customer_id VARCHAR(255),
        PRIMARY KEY (sale_id),
        FOREIGN KEY (unique_customer_id) REFERENCES dim_customer(unique_id),
        FOREIGN KEY (store_id) REFERENCES dim_locations(id)
      )
    """
)

print("fact_apj_sale_items created.")

# COMMAND ----------

# DBTITLE 1,Create fact_apj_sales
spark.sql(
    """
      CREATE TABLE fact_apj_sales (
        sale_id STRING,
        ts TIMESTAMP,
        order_source STRING,
        order_state STRING,
        unique_customer_id STRING,
        store_id STRING,
        customer_skey INT,
        slocation_skey INT,
        PRIMARY KEY (sale_id),
        FOREIGN KEY (unique_customer_id) REFERENCES dim_customer(unique_id)
      )
    """
)

print("fact_apj_sales created.")

# COMMAND ----------

# DBTITLE 1,Define read file function


def read_file_contents(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


# COMMAND ----------

# DBTITLE 1,Insert dim_customer
spark.sql(read_file_contents("../Datasets/SQL Lab/dim_customer.sql"))

print("dim_customer records inserted.")

# COMMAND ----------

# DBTITLE 1,Insert dim_locations
spark.sql(read_file_contents("../Datasets/SQL Lab/dim_locations.sql"))

print("dim_locations records inserted.")

# COMMAND ----------

# DBTITLE 1,Insert dim_products
spark.sql(read_file_contents("../Datasets/SQL Lab/dim_products.sql"))

print("dim_products records inserted.")

# COMMAND ----------

# DBTITLE 1,Insert fact_apj_sales
spark.sql(read_file_contents("../Datasets/SQL Lab/fact_apj_sales.sql"))

print("fact_apj_sales records inserted.")

# COMMAND ----------

# DBTITLE 1,Insert fact_apj_sale_items
spark.sql(read_file_contents("../Datasets/SQL Lab/fact_apj_sale_items.sql"))

print("fact_apj_sale_items records inserted.")

# COMMAND ----------

# DBTITLE 1,Create store_data_json
spark.sql(
    """
      CREATE OR REPLACE TABLE store_data_json AS
        SELECT
          1 AS id,
          parse_json('{"store":{"fruit":[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}],"basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],"book":[{"author":"Nigel Rees","title":"Sayings of the Century","category":"reference","price":8.95},{"author":"Herman Melville","title":"Moby Dick","category":"fiction","price":8.99,"isbn":"0-553-21311-3"},{"author":"J. R. R. Tolkien","title":"The Lord of the Rings","category":"fiction","reader":[{"age":25,"name":"bob"},{"age":26,"name":"jack"}],"price":22.99,"isbn":"0-395-19395-8"}],"bicycle":{"price":19.95,"color":"red"}},"owner":"amy","zip code":"94025","fb:testid":"1234"}') as raw
    """
)

print("store_data_json created.")
