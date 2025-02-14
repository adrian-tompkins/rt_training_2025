-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Explore Datasets
-- MAGIC
-- MAGIC Explore the datasets we will be using for generating the silver and gold tables.

-- COMMAND ----------

SELECT * FROM rtio_dataproducts.mining.hme_time_usage_event

-- COMMAND ----------

SELECT COUNT(*) FROM rtio_dataproducts.mining.hme_time_usage_event

-- COMMAND ----------

SELECT * FROM rtio_dataproducts.mining.hme_time_usage_classification

-- COMMAND ----------

SELECT COUNT(*) FROM rtio_dataproducts.mining.hme_time_usage_classification

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ask Databricks Assistant to help with creating a Silver Table
-- MAGIC
-- MAGIC Below is a query that joins the hme time usage event and classification tables together
-- MAGIC
-- MAGIC Use the assistant to help tweak the query. This could include
-- MAGIC  - Selecting only active records
-- MAGIC  - Selecting a subset of columns
-- MAGIC  - Changing the columns names
-- MAGIC  - Translating the code to python (hint, change the cell type to "python")
-- MAGIC
-- MAGIC In general, as an end result you will probably want a table that has at least the site, asset id, duration time, event date / time and tum7 display name columns.
-- MAGIC
-- MAGIC Once your happy with the result, write your table out to the catalog (template at the bottom of the notebook)
-- MAGIC
-- MAGIC Feel free to explore the `mining` or other schemas in the `rtio_dataproducts` catalog and bulid your own silver tables
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM rtio_dataproducts.mining.hme_time_usage_event tue 
JOIN rtio_dataproducts.mining.hme_time_usage_classification tuc
ON tuc.hme_time_usage_classification_bk = tue.time_usage_classification_bk

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_labs.<my_schema>.silver_hme_event
AS
-- Paste SELECT statement here
