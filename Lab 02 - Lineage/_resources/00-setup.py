# Databricks notebook source
current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
uc_catalog = 'apjworkshop24'
database_name = current_user_id.split('@')[0].replace('.','_')


# COMMAND ----------

spark.sql(f"""USE CATALOG {uc_catalog}""")

# COMMAND ----------



db_not_exist = len([db for db in spark.catalog.listDatabases() if db.name == f'{database_name}']) == 0
if db_not_exist:
  print("creating database")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name} ")
  spark.sql(f"GRANT CREATE, USAGE on DATABASE {database_name} TO `account users`")


# COMMAND ----------

spark.sql(f'USE {database_name}')
spark.sql(f"CREATE TABLE IF NOT EXISTS {database_name}.dinner ( recipe_id INT, full_menu STRING)");
spark.sql(f"CREATE TABLE IF NOT EXISTS {database_name}.dinner_price ( recipe_id INT, full_menu STRING, price DOUBLE)");
spark.sql(f"CREATE TABLE IF NOT EXISTS {database_name}.menu ( recipe_id INT, app STRING, main STRING, desert STRING)");
spark.sql(f"CREATE TABLE IF NOT EXISTS {database_name}.price ( recipe_id BIGINT, price DOUBLE)");

# COMMAND ----------

if db_not_exist:
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE {database_name}.dinner TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE {database_name}.dinner_price TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE {database_name}.menu TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE {database_name}.price TO `account users`")
