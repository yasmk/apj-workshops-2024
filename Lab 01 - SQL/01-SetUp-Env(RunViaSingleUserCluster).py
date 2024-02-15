# Databricks notebook source
import os

reset = True
current_user_id = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)
datasets_location = f"/FileStore/tmp/{current_user_id}/datasets/"
catalog = "apjworkshop24"
database_name = current_user_id.split("@")[0].replace(".", "_")

# create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")
spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} to `{current_user_id}`")
spark.sql(f"GRANT CREATE SCHEMA ON CATALOG {catalog} to `{current_user_id}`")
spark.sql(f"USE CATALOG {catalog};")

if reset:
    dbutils.fs.rm(datasets_location, True)
    spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")


# create database
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name};")
spark.sql(f"USE {database_name}")

# COMMAND ----------

working_dir = "/".join(os.getcwd().split("/")[0:5])
git_datasets_location = f"{working_dir}/Datasets/SQL Lab"

sample_datasets = [
    "dim_customer",
    "dim_locations",
    "fact_apj_sale_items",
    "fact_apj_sales",
]
for sample_data in sample_datasets:
    dbutils.fs.rm(f"{datasets_location}/SQL_Lab/{sample_data}.csv.gz")
    dbutils.fs.cp(
        f"file:{git_datasets_location}/{sample_data}.csv.gz",
        f"{datasets_location}/SQL_Lab/{sample_data}.csv.gz",
    )

# COMMAND ----------

dbutils.fs.ls(f"{datasets_location}/SQL_Lab/")


# COMMAND ----------

# MAGIC %md
# MAGIC ###GET the DATABASE NAME below
# MAGIC You should use this throughout the lab

# COMMAND ----------

print(f"Use this catalog.database name through out the lab: {catalog}.{database_name}")

# COMMAND ----------

table_name = "dim_products"
sample_file = f"{table_name}.csv.gz"
spark.conf.set("sampledata.path", f"dbfs:{datasets_location}SQL_Lab/{sample_file}")
spark.conf.set("table.name", table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `${table.name}`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `${table.name}`;
# MAGIC
# MAGIC COPY INTO `${table.name}` FROM
# MAGIC (SELECT *
# MAGIC FROM  '${sampledata.path}')
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true',
# MAGIC                 'delimiter' = ',',
# MAGIC                 'header' = 'true',
# MAGIC                 'quote'="'")
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC SELECT * FROM `${table.name}`;

# COMMAND ----------

table_name = "dim_locations"
sample_file = f"{table_name}.csv.gz"
spark.conf.set("sampledata.path", f"dbfs:{datasets_location}SQL_Lab/{sample_file}")
spark.conf.set("table.name", table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `${table.name}`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `${table.name}`;
# MAGIC
# MAGIC COPY INTO `${table.name}` FROM
# MAGIC (SELECT *
# MAGIC FROM  '${sampledata.path}')
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true',
# MAGIC                 'delimiter' = ',',
# MAGIC                 'header' = 'true',
# MAGIC                 'quote'="'")
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC SELECT * FROM `${table.name}`;

# COMMAND ----------

table_name = "fact_apj_sales"
sample_file = f"{table_name}.csv.gz"
spark.conf.set("sampledata.path", f"dbfs:{datasets_location}SQL_Lab/{sample_file}")
spark.conf.set("table.name", table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `${table.name}`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `${table.name}`;
# MAGIC
# MAGIC COPY INTO `${table.name}` FROM
# MAGIC (SELECT *
# MAGIC FROM  '${sampledata.path}')
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true',
# MAGIC                 'delimiter' = ',',
# MAGIC                 'header' = 'true',
# MAGIC                 'quote'="'")
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC SELECT * FROM `${table.name}`;

# COMMAND ----------

table_name = "fact_apj_sale_items"
sample_file = f"{table_name}.csv.gz"
spark.conf.set("sampledata.path", f"dbfs:{datasets_location}SQL_Lab/{sample_file}")
spark.conf.set("table.name", table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `${table.name}`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `${table.name}`;
# MAGIC
# MAGIC COPY INTO `${table.name}` FROM
# MAGIC (SELECT *
# MAGIC FROM  '${sampledata.path}')
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true',
# MAGIC                 'delimiter' = ',',
# MAGIC                 'header' = 'true',
# MAGIC                 'quote'="'")
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC SELECT * FROM `${table.name}`;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*store_data, json*/
# MAGIC CREATE OR REPLACE TABLE store_data_json
# MAGIC AS SELECT
# MAGIC 1 AS id,
# MAGIC '{
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
# MAGIC
# MAGIC SELECT * FROM store_data_json;
# MAGIC

# COMMAND ----------

print(f"Use this catalog.database name through out the lab: {catalog}.{database_name}")
