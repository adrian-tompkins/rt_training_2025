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

generate_sales_dataset()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configure DLT Pipeline
# MAGIC
# MAGIC Have a go a creating the DLT pipeline. You will need to set the pipeline to
# MAGIC  - Be Serverless
# MAGIC  - Use the `lakehouse_labs` catalog
# MAGIC  - Use your schema
# MAGIC  - Use the `2 - Transform` notebook
# MAGIC  - Configure the **Advanced -> Configuration** option as below...

# COMMAND ----------

displayHTML("""<b>Advanced -> Configuration:</b>""")
displayHTML("""Key: <b style="color:green">schema_name</b>""")
displayHTML("""Value: <b style="color:green">{}</b>""".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create and Run your Pipeline NOW
# MAGIC
# MAGIC While waiting for the pipeline to execute - explore `02 - Transform` notebook to see the actual code used to create it.

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
