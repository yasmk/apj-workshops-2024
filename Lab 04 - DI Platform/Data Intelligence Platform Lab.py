# Databricks notebook source
# MAGIC %md
# MAGIC # Data Intelligence Platform Lab
# MAGIC
# MAGIC ## Getting Started
# MAGIC
# MAGIC ### By the end of this lab you will have learned:
# MAGIC
# MAGIC 1. How to upload data to a Unity Catalog Volume <br />
# MAGIC More information about Unity Catalog Volumes: 
# MAGIC
# MAGIC 2. How to use the Databricks assistant to help you combine and analyse data in Databricks
# MAGIC
# MAGIC 3. How to create new tables using the UI 
# MAGIC
# MAGIC 4. How to build and orchestrate ETL pipelines with low-code/no-code steps
# MAGIC
# MAGIC 5. How to build a report using Databricks Lakeview
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Upload a File to Unity Catalog Volume - BYO Data - and create a new table
# MAGIC
# MAGIC
# MAGIC 1. Download the file `https://github.com/yasmk/apj-workshops-2024/blob/main/Datasets/DI%20Platform%20Lab/product_descriptions.tsv` to your local computer <br />
# MAGIC 2. Navigate to the `Catalog Explorer` and find the `byo_data` volume under your catalog and schema
# MAGIC <br /><img style="float:right" src="https://raw.githubusercontent.com/yasmk/apj-workshops-2024/feature/di-platform-lab1/Resources/Screenshots/1.2.png"/><br />
# MAGIC 3. Click on the volume name and then click on `"Upload to this volume"` button on the top right corner<br />
# MAGIC 4. Select the `product_descriptions.tsv` from your local computer and upload to the volume<br />
# MAGIC 5. After the upload completes, click on the 3 dot button (vertical elipsis) on the far right side of the file name and select `create table`
# MAGIC <br /><img style="float:right" src="https://raw.githubusercontent.com/yasmk/apj-workshops-2024/feature/di-platform-lab1/Resources/Screenshots/1.5.png"/><br />
# MAGIC 6. Leave the `create new table` option selected, then select the correct catalog and schema names from the drop down. Keep the table name as `product_description`<br />
# MAGIC 7. Examine the available data, then click `create table`<br />
# MAGIC 8. Once the table creation is complete, click on the `Sample Data` tab in the `Catalog Explorer` to see if the data was loaded correctly and is displayed as expected. <br />
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ask Databricks Assistant to help you write a join between new and existing data
# MAGIC
# MAGIC 1. Open the `SQL Editor` on the Databricks Workspace menu and create a new query tab<br /><br />
# MAGIC 2. Select the correct catalog and schema at the top drop down menu<br /><br />
# MAGIC 3. Click on the Databricks Assistant icon on the left hand side of the SQL Editor window and type the prompt: `How can I join the tables dim_products and product_description to add descriptions to my products?`<br />
# MAGIC As the Databricks Assistant can provide different results every time you aske the same question, go ahead and do a few more prompt iterations to get to a usable query, if necessary. If the Assistant can give you an `INNER JOIN` query where you can replace the column names, go ahead and adjust accordingly: <br /><br />
# MAGIC
# MAGIC The final query should be similar to this:
# MAGIC
# MAGIC ```
# MAGIC SELECT p.*, pd.prod_desc
# MAGIC FROM apjworkshop24.will_scalioni.dim_products AS p
# MAGIC JOIN apjworkshop24.will_scalioni.product_description AS pd ON p.name = pd.prod_name
# MAGIC ```
# MAGIC
# MAGIC 4. Click on the `Save*` button at the top of the SQL Editor, name the query `Products with full Description`, and save it to your Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ask Databricks Assistant to help with creating a Silver Table
# MAGIC
# MAGIC 1. Create a new Notebook and set the default language to SQL
# MAGIC 2. You can ask the assistant to help you build a query to identify the top completed orders (highest to lowest order total in dollars) by leveraging the inline assistant within the notebook cell. Or you can use the query below. Make sure you update the catalog and schema name accordingly. 
# MAGIC
# MAGIC ```
# MAGIC select 
# MAGIC     a.ts,
# MAGIC     a.sale_id,
# MAGIC     a.order_source,
# MAGIC     a.order_state,
# MAGIC     a.unique_customer_id,
# MAGIC     a.customer_skey,
# MAGIC     a.store_id,
# MAGIC     a.slocation_skey,
# MAGIC     sum(b.product_cost) order_total
# MAGIC from `apjworkshop24`.`will_scalioni`.`fact_apj_sales` a 
# MAGIC     inner join `apjworkshop24`.`will_scalioni`.`fact_apj_sale_items` b
# MAGIC     on a.sale_id = b.sale_id
# MAGIC where 
# MAGIC     a.order_state = 'COMPLETED'
# MAGIC group by
# MAGIC     all
# MAGIC order by 
# MAGIC     order_total desc;
# MAGIC ```
# MAGIC
# MAGIC 3. Edit the query and _create or replace_ a Silver table to persist the results in the Lakehouse as a Delta Table. Name the new table `top_orders_silver`. Make sure you create it in the correct catalog and schema. 
# MAGIC
# MAGIC 4. Add a new cell to the notebook and run a `select * from` the new table so you can check the results in the same notebook. 
# MAGIC
# MAGIC 5. Rename the Notebook to `01 - Data Preparation Silver`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ask Databricks Assistant to help with creating a Gold Table
# MAGIC
# MAGIC 1. Create a new Notebook and set the default language to SQL
# MAGIC 2. You can ask the assistant to help you build a query to aggregate the total sales by store and date, by leveraging the inline assistant within the notebook cell. Or you can use the query below. Make sure you update the catalog and schema name accordingly.
# MAGIC
# MAGIC ```
# MAGIC select  
# MAGIC     date(ts) as order_date,
# MAGIC     store_id,
# MAGIC     sum(order_total) total_sales_by_store
# MAGIC from 
# MAGIC     `apjworkshop24`.`will_scalioni`.`top_orders_silver` 
# MAGIC group by all
# MAGIC ```
# MAGIC
# MAGIC 3. Edit the query and create or replace a Gold table to persist the results in the Lakehouse as a Delta Table. Name the new table `top_stores_gold`. Make sure you create it in the correct catalog and schema.
# MAGIC
# MAGIC 3. Rename the Notebook to `02 - Data Preparation Gold`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create a Workflow to orchestrate your pipeline
# MAGIC
# MAGIC 1. Open the Workflows page in the Databricks Workspace and click the `Create Job` button at the top right corner
# MAGIC
# MAGIC 2. Configure the first task as follows:
# MAGIC - Task Name `Data_Preparation_Silver` 
# MAGIC - Type: Notebook
# MAGIC - Source: Workspace
# MAGIC - Path: navigate and select the `01 - Data Preparation Silver` notebook 
# MAGIC - Compute: select the DBSQL Serverless cluster
# MAGIC - Click `Create Task`
# MAGIC
# MAGIC 3. Add a second task to the same Workflow, and set it as a conditional check: `if/else condition`
# MAGIC - Task name: `Status Check`
# MAGIC - Condition: Click on the link `Browse dynamic values` and copy ``{{tasks.[task_name].result_state}}``
# MAGIC - Replace the task name accordingly replacing the placeholder with the correct previous task name: `{{tasks.Data_Preparation_Silver.result_state}}`
# MAGIC
# MAGIC 5. Add a third task to the Workflow and configure it as follows:
# MAGIC - Task Name `Data_Preparation_Gold` 
# MAGIC - Type: Notebook
# MAGIC - Source: Workspace
# MAGIC - Path: navigate and select the `02 - Data Preparation Gold` notebook 
# MAGIC - Compute: select the DBSQL Serverless cluster
# MAGIC - Click `Create Task`
# MAGIC
# MAGIC 6. At the top of the Workflow definition window, rename the Workflow from `New Job [timestamp]` to a meaningful name - i.e.: `Sales Pipeline`
# MAGIC
# MAGIC 7. Click `Run Now`, then `View Run` to examine the job execution. Validate the job executed successfully. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create a Lakeview report based on the new dataset
# MAGIC
# MAGIC 1. Open the Dashboards from the left menu and click on the Lakeview Dashboards tab
# MAGIC 2. Click on `Create Lakeview Dashboard` button on the top right corner
# MAGIC 3. Switch from the Canvas tab to the Data tab
# MAGIC 4. Click on the Select a table button, select your catalog and schema, then click on the `top_orders_silver` table and wait for the results to show
# MAGIC 5. Repeat the steps above and add the table `top_stores_gold` and wait for the results to show
# MAGIC 6. Switch back to the Canvas tab and click on the `Add a Visualization` button on the blue bar at the bottom of the Canvas
# MAGIC 7. Select any position for your visualization and describe what you want to see. Feel free to use some of the prompt suggestions below or try your own ideas based on the available data. 
# MAGIC - `Show me the split between order source types`
# MAGIC - `What are the top performing stores?`
# MAGIC 8. Add a new visualization and experiment with creating a visual manually without using the GenAI prompt, play with the Visualization configuration
# MAGIC
