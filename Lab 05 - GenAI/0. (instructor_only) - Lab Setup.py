# Databricks notebook source
# MAGIC %md
# MAGIC # Setting up Training Lab
# MAGIC
# MAGIC This notebook is to be run by the instructor only to load documents and setup the lab utilities

# COMMAND ----------

# MAGIC # last tested with 0.23 of the databricks-vectorsearch client
# MAGIC %pip install --upgrade --force-reinstall databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
vsc_endpoint_name = 'workshop-vs-endpoint'

db_catalog = 'gen_ai_catalog'
db_schema = 'lab_05'
volume_name = 'source_files'

# docs to load
pdfs = {'2203.02155.pdf':'https://arxiv.org/pdf/2203.02155.pdf',
        '2302.09419.pdf': 'https://arxiv.org/pdf/2302.09419.pdf',
        'Brooks_InstructPix2Pix_Learning_To_Follow_Image_Editing_Instructions_CVPR_2023_paper.pdf': 'https://openaccess.thecvf.com/content/CVPR2023/papers/Brooks_InstructPix2Pix_Learning_To_Follow_Image_Editing_Instructions_CVPR_2023_paper.pdf',
        '2303.10130.pdf':'https://arxiv.org/pdf/2303.10130.pdf',
        '2302.06476.pdf':'https://arxiv.org/pdf/2302.06476.pdf',
        '2302.06476.pdf':'https://arxiv.org/pdf/2302.06476.pdf',
        '2303.04671.pdf':'https://arxiv.org/pdf/2303.04671.pdf',
        '2209.07753.pdf':'https://arxiv.org/pdf/2209.07753.pdf',
        '2302.07842.pdf':'https://arxiv.org/pdf/2302.07842.pdf',
        '2302.07842.pdf':'https://arxiv.org/pdf/2302.07842.pdf',
        '2204.01691.pdf':'https://arxiv.org/pdf/2204.01691.pdf'}

# COMMAND ----------

# DBTITLE 1,Start Endpoint
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

vsc.create_endpoint(
    name=vsc_endpoint_name,
    endpoint_type="STANDARD"
)

# COMMAND ----------

# DBTITLE 1,Setting up sample data sources
# We will use UC Volumes for this

spark.sql(f"CREATE CATALOG IF NOT EXISTS {db_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_catalog}.{db_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {db_catalog}.{db_schema}.{volume_name}")

volume_folder = f'/Volumes/{db_catalog}/{db_schema}/{volume_name}/'

# COMMAND ----------
import os
import requests
user_agent = "me-me-me"

def load_file(file_uri, file_name, library_folder):
    
    # Create the local file path for saving the PDF
    local_file_path = os.path.join(library_folder, file_name)

    # Download the PDF using requests
    try:
        # Set the custom User-Agent header
        headers = {"User-Agent": user_agent}

        response = requests.get(file_uri, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Save the PDF to the local file
            with open(local_file_path, "wb") as pdf_file:
                pdf_file.write(response.content)
            print("PDF downloaded successfully.")
        else:
            print(f"Failed to download PDF. Status code: {response.status_code}")
    except requests.RequestException as e:
        print("Error occurred during the request:", e)

for pdf in pdfs.keys():
    load_file(pdfs[pdf], pdf, volume_folder)




