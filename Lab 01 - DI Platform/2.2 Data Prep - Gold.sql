-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create a Gold Table
-- MAGIC
-- MAGIC Have a go at creating a gold table, using the databricks assistant to help. You may want to try something like asking the assistant to change the grain of the silver_hme_event table that was created, summing over the duration, grouping by site, etc.
-- MAGIC
-- MAGIC Once you are happy, write the table out.
-- MAGIC
-- MAGIC Feel free to add more gold tables if desired.
-- MAGIC

-- COMMAND ----------

SELECT * FROM lakehouse_labs.<my_schema>.silver_hme_event

-- COMMAND ----------

CREATE TABLE lakehouse_labs.<my_schema>.gold_hme
AS
-- Write select query here
-- Alternatively, try using python and the pyspark api's instead to write a table
