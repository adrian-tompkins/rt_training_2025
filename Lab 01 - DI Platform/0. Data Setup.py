# Databricks notebook source
import os

current_user_id = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)

catalog = "lakehouse_labs"
database_name = current_user_id.split("@")[0].replace(".", "_").replace("-", "_") + "_db_training"
volume = "byo_data"

datasets_source = f"/Volumes/adrian_tompkins/training/data"
# datasets_location = f"/Volumes/{catalog}/{database_name}/{volume}/datasets/"


spark.sql(f"USE CATALOG {catalog};")
spark.sql(f"USE SCHEMA {database_name};")


# create volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###GET the DATABASE NAME below
# MAGIC You should use this throughout the lab

# COMMAND ----------

print(f"Use this catalog.database name through out the lab: {catalog}.{database_name}")

# COMMAND ----------

datasets = [
  "dim_customer",
  "dim_locations",
  "dim_products",
  "fact_apj_sales",
  "fact_apj_sale_items"
]
for dataset in datasets:
  (spark.read.csv(f"{datasets_source}/SQL Lab/{dataset}.csv.gz", header=True)
    .write
    .mode("overwrite")
    .saveAsTable(dataset)
  )

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
