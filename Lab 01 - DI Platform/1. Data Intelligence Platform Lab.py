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
# MAGIC ## 0. Lab Setup
# MAGIC Run the `0. Data Setup` notebook to setup the lab environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Upload a File to Unity Catalog Volume - BYO Data - and create a new table
# MAGIC
# MAGIC
# MAGIC 1. Download the [Product Description](https://github.com/adrian-tompkins/apj-workshops-2024/blob/main/Datasets/DI%20Platform%20Lab/product_description.tsv) file to your local computer <br />
# MAGIC 2. Navigate to the `Catalog Explorer` and find the `byo_data` volume under your catalog and schema
# MAGIC <br /><img style="float:right" src="https://github.com/adrian-tompkins/apj-workshops-2024/blob/main/Resources/Screenshots/1.2.png?raw=true"/><br />
# MAGIC 3. Click on the volume name and then click on `"Upload to this volume"` button on the top right corner<br />
# MAGIC 4. Select the `product_description.tsv` file from your local computer and upload to the volume<br />
# MAGIC 5. After the upload completes, click on the 3 dot button (vertical elipsis) on the far right side of the file name and select `create table`
# MAGIC <br /><img style="float:right" src="https://github.com/adrian-tompkins/apj-workshops-2024/blob/main/Resources/Screenshots/1.5.png?raw=true"/><br />
# MAGIC 6. Leave the `create new table` option selected, then select the correct catalog and schema names from the drop down. Leave the table name as product_description<br />
# MAGIC 7. Examine the available data, then click `create table`<br />
# MAGIC 8. Once the table creation is complete, click on the `Sample Data` tab in the `Catalog Explorer` to see if the data was loaded correctly and is displayed as expected. <br />
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Add Comments and Discover Data
# MAGIC Add some comments to the `product_description` table that will help others find it, using the AI suggested comments to help out. Test out searching for this table using the "search data" bar at the top of the screen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ask Databricks Assistant to help you write a join between new and existing data
# MAGIC
# MAGIC 1. Open the `SQL Editor` on the Databricks Workspace menu and create a new query tab<br /><br />
# MAGIC 2. Select the correct catalog and schema at the top drop down menu<br /><br />
# MAGIC 3. Click on the Databricks Assistant icon on the left hand side of the SQL Editor window and type the prompt: **How can I combine the tables dim_products and product_description together, to add descriptions to my products? The products "id" column is related to the product description "prod_name" column. Please bring back all columns in the result.**<br />
# MAGIC
# MAGIC
# MAGIC The final query should be similar to this:
# MAGIC
# MAGIC ```
# MAGIC SELECT p.*, pd.prod_desc
# MAGIC FROM lakehouse_labs.<user_name>.dim_products AS p
# MAGIC JOIN lakehouse_labs.<user_name>.product_description AS pd ON p.name = pd.prod_name
# MAGIC ```
# MAGIC 4. Try asking the assistant different questions to make alterations to the query, such as changing what columns are selected, or filtering on rows. Try making a query that's syntactically incorrect, and see if the assistant can help you fix it.
# MAGIC
# MAGIC 5. To save the query click on the `Save*` button at the top of the SQL Editor, cive the query a name, and save it to your Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explore Some Rio Tinto Data
# MAGIC
# MAGIC Using the SQL Editor, explore some data in the `rtio_dataproducts.mining` schema. What do you find there? We will be using data in this schema for the following tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build a Silver and Gold Table
# MAGIC
# MAGIC Open the `2.1 Data Prep - Silver` notebook and follow the instructions to create a silver table.
# MAGIC
# MAGIC Similarity, open the `2.2 Data Prep - Gold` notebook and follow the instructions to create a gold table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create a Workflow to orchestrate your pipeline
# MAGIC
# MAGIC 1. Open the Workflows page in the Databricks Workspace and click the `Create Job` button at the top right corner
# MAGIC
# MAGIC 2. Configure the first task as follows:
# MAGIC - Task Name `Data_Preparation_Silver` 
# MAGIC - Type: Notebook
# MAGIC - Source: Workspace
# MAGIC - Path: navigate and select the `2.1 Data Prep - Silver` notebook 
# MAGIC - Compute: select the DBSQL Serverless cluster
# MAGIC - Click `Create Task`
# MAGIC
# MAGIC 3. Add a second task to the Workflow and configure it as follows:
# MAGIC - Task Name `Data_Preparation_Gold` 
# MAGIC - Type: Notebook
# MAGIC - Source: Workspace
# MAGIC - Path: navigate and select the `2.2 Data Prep - Gold` notebook 
# MAGIC - Compute: select the DBSQL Serverless cluster
# MAGIC - Click `Create Task`
# MAGIC
# MAGIC 4. At the top of the Workflow definition window, rename the Workflow from `New Job [timestamp]` to a meaningful name - i.e.: `Sales Pipeline`
# MAGIC
# MAGIC 5. Click `Run Now`, then `View Run` to examine the job execution. Validate the job executed successfully. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create a Lakeview report based on the new dataset
# MAGIC
# MAGIC 1. Open the Dashboards from the left menu and click on the Lakeview Dashboards tab
# MAGIC 2. Click on `Create Lakeview Dashboard` button on the top right corner
# MAGIC 3. Switch from the Canvas tab to the Data tab
# MAGIC 4. Click on the Select a table button, select your catalog and schema, then click on the your gold table that you created.
# MAGIC 5. Repeat the steps above for adding addional gold or silver tables.
# MAGIC 6. Switch back to the Canvas tab and click on the `Add a Visualization` button on the blue bar at the bottom of the Canvas
# MAGIC 7. Select any position for your visualization and describe what you want to see
# MAGIC 8. Add a new visualization and experiment with creating a visual manually without using the GenAI prompt, play with the Visualization configuration
# MAGIC
