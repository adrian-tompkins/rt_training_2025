# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Introduction

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Delta Live Tables (DLT) makes it easy to build and manage reliable data pipelines that deliver high quality data on Delta Lake. 
# MAGIC
# MAGIC DLT helps data engineering teams simplify ETL development and management with declarative pipeline development, automatic data testing, and deep visibility for monitoring and recovery.
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/09/Live-Tables-Pipeline.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Set up Environment

# COMMAND ----------

# MAGIC %run ../Utils/prepare-lab-environment

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configure DLT Pipeline
# MAGIC
# MAGIC Pipeline code is stored in a different notebook. This notebook will help you get some custom values needed to create DLT Pipeline.
# MAGIC
# MAGIC To run this lab we need to use standardized values for  **Target** and  **Storage Location** and **Configuration** .
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC Run the cell bellow and use values returned. They will be specific to your lab environment.

# COMMAND ----------

storage_location = f'/FileStore/tmp/{current_user_id}/dlt_pipeline'
pipline_name = f"{current_user_id}_pipeline"
notebook_path = f"/Repos/{current_user_id}/apj-workshops-2024/Lab 06 - Data Engineering/01 - DLT/2 - Transform"

displayHTML("""<h2>Use these values to create your Delta Live Pipeline</h2>""")
displayHTML("""<b>Pipeline name: </b>""")
displayHTML(f"""<b style="color:green">{pipline_name}</b>""")


displayHTML("""<b>Pipeline Mode: </b>""")
displayHTML(f"""<b style="color:green">Triggered</b>""")

displayHTML("""<b>Noteook path: </b>""")
displayHTML(f"""<b style="color:green">{notebook_path}</b>""")

displayHTML("""<b>Destination:</b>""")
displayHTML("""<b style="color:green">Unity Catalog: apjworkshop24</b>""")
displayHTML("""<b>Target Schema:</b>""")
displayHTML("""<b style="color:green">{}</b>""".format(database_name))
displayHTML("""<b>Advanced -> Configuration:</b>""")
displayHTML("""Key: <b style="color:green">current_user_id</b>""")
displayHTML("""Value: <b style="color:green">{}</b>""".format(current_user_id))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create and Run your Pipeline NOW
# MAGIC
# MAGIC It will take some time for pipeline to start. While waiting - explore `02 - Transform` notebook to see the actual code used to create it.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Incremental Updates

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Simulate new batch files being uploaded to cloud location. You can run it multiple times - it will generate a sample of orders for randomly selected store.
# MAGIC
# MAGIC If pipeline is running in continuous mode - files will be processed as soon as they are uploaded. Otherwise new files will be picked up on the next run.

# COMMAND ----------

generate_more_orders()
