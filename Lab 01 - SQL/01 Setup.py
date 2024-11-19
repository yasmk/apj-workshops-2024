# Databricks notebook source
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

# COMMAND ----------

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
                print(f"Updated file: {file_path}")


directory_path = f"/Workspace/Users/{current_user_id}/apj-workshops-2024"
replace_in_files(directory_path, "catalog.database", f"{catalog}.{database_name}")
replace_in_files(directory_path, "email_address", current_user_id)

# COMMAND ----------

# DBTITLE 1,Create dim_customer
# MAGIC %sql
# MAGIC CREATE TABLE dim_customer (
# MAGIC   unique_id VARCHAR(50) NOT NULL,
# MAGIC   pk INT NOT NULL,
# MAGIC   id INT NOT NULL,
# MAGIC   store_id VARCHAR(50) NOT NULL,
# MAGIC   name VARCHAR(255),
# MAGIC   email VARCHAR(255),
# MAGIC   PRIMARY KEY (unique_id)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Create dim_locations
# MAGIC %sql
# MAGIC CREATE TABLE dim_locations (
# MAGIC   id VARCHAR(50) NOT NULL,
# MAGIC   pk INT NOT NULL,
# MAGIC   name VARCHAR(255),
# MAGIC   email VARCHAR(255),
# MAGIC   city VARCHAR(100),
# MAGIC   hq VARCHAR(255),
# MAGIC   phone_number VARCHAR(50),
# MAGIC   country_code VARCHAR(10),
# MAGIC   PRIMARY KEY (id)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Create dim_products
# MAGIC %sql
# MAGIC CREATE TABLE dim_products (
# MAGIC   product_skey INT NOT NULL,
# MAGIC   id VARCHAR(255) NOT NULL,
# MAGIC   ingredients STRING,
# MAGIC   name VARCHAR(255),
# MAGIC   current_record CHAR(1),
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   PRIMARY KEY (id)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Create fact_apj_sale_items
# MAGIC %sql
# MAGIC CREATE TABLE fact_apj_sale_items (
# MAGIC     sale_id VARCHAR(255) NOT NULL,
# MAGIC     product_id VARCHAR(255),
# MAGIC     store_id VARCHAR(50),
# MAGIC     product_size VARCHAR(50),
# MAGIC     product_cost DECIMAL(10, 2),
# MAGIC     product_ingredients STRING,
# MAGIC     product_skey INT,
# MAGIC     slocation_skey INT,
# MAGIC     unique_customer_id VARCHAR(255),
# MAGIC     PRIMARY KEY (sale_id),
# MAGIC     FOREIGN KEY (unique_customer_id) REFERENCES dim_customer(unique_id),
# MAGIC     FOREIGN KEY (store_id) REFERENCES dim_locations(id)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE fact_apj_sales (
# MAGIC     sale_id STRING,
# MAGIC     ts TIMESTAMP,
# MAGIC     order_source STRING,
# MAGIC     order_state STRING,
# MAGIC     unique_customer_id STRING,
# MAGIC     store_id STRING,
# MAGIC     customer_skey INT,
# MAGIC     slocation_skey INT,
# MAGIC     PRIMARY KEY (sale_id),
# MAGIC     FOREIGN KEY (unique_customer_id) REFERENCES dim_customer(unique_id)
# MAGIC )

# COMMAND ----------


# DBTITLE 1,Define read file function
def read_file_contents(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


# COMMAND ----------

# DBTITLE 1,Insert dim_customer
spark.sql(read_file_contents("../Datasets/SQL Lab/dim_customer.sql"))

# COMMAND ----------

# DBTITLE 1,Insert dim_locations
spark.sql(read_file_contents("../Datasets/SQL Lab/dim_locations.sql"))

# COMMAND ----------

# DBTITLE 1,Insert dim_products
spark.sql(read_file_contents("../Datasets/SQL Lab/dim_products.sql"))

# COMMAND ----------

# DBTITLE 1,Insert fact_apj_sales
spark.sql(read_file_contents("../Datasets/SQL Lab/fact_apj_sales.sql"))

# COMMAND ----------

# DBTITLE 1,Insert fact_apj_sale_items
spark.sql(read_file_contents("../Datasets/SQL Lab/fact_apj_sale_items.sql"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE store_data_json AS
# MAGIC SELECT
# MAGIC   1 AS id,
# MAGIC   '{
# MAGIC    "store":{
# MAGIC       "fruit": [
# MAGIC         {"weight":8,"type":"apple"},
# MAGIC         {"weight":9,"type":"pear"}
# MAGIC       ],
# MAGIC       "basket":[
# MAGIC         [1,2,{"b":"y","a":"x"}],
# MAGIC         [3,4],
# MAGIC         [5,6]
# MAGIC       ],
# MAGIC       "book":[
# MAGIC         {
# MAGIC           "author":"Nigel Rees",
# MAGIC           "title":"Sayings of the Century",
# MAGIC           "category":"reference",
# MAGIC           "price":8.95
# MAGIC         },
# MAGIC         {
# MAGIC           "author":"Herman Melville",
# MAGIC           "title":"Moby Dick",
# MAGIC           "category":"fiction",
# MAGIC           "price":8.99,
# MAGIC           "isbn":"0-553-21311-3"
# MAGIC         },
# MAGIC         {
# MAGIC           "author":"J. R. R. Tolkien",
# MAGIC           "title":"The Lord of the Rings",
# MAGIC           "category":"fiction",
# MAGIC           "reader":[
# MAGIC             {"age":25,"name":"bob"},
# MAGIC             {"age":26,"name":"jack"}
# MAGIC           ],
# MAGIC           "price":22.99,
# MAGIC           "isbn":"0-395-19395-8"
# MAGIC         }
# MAGIC       ],
# MAGIC       "bicycle":{
# MAGIC         "price":19.95,
# MAGIC         "color":"red"
# MAGIC       }
# MAGIC     },
# MAGIC     "owner":"amy",
# MAGIC     "zip code":"94025",
# MAGIC     "fb:testid":"1234"
# MAGIC  }' as raw;
