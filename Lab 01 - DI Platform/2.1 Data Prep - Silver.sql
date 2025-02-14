-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Explore Datasets
-- MAGIC
-- MAGIC Explore the datasets we will be using for generating the silver and gold tables.
-- MAGIC
-- MAGIC Relpace `<my_schema>` with the schema your personal schema

-- COMMAND ----------

select * from adrian_tompkins.adrian_tompkins.fact_apj_sales

-- COMMAND ----------

select * from adrian_tompkins.adrian_tompkins.fact_apj_sale_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ask Databricks Assistant to help with creating a Silver Table
-- MAGIC
-- MAGIC Ask the assistant to help you build a query to identify the top completed orders (highest to lowest order total in dollars) by leveraging the inline assistant within the notebook cell.
-- MAGIC
-- MAGIC Here's an example prompt to help out:
-- MAGIC
-- MAGIC **Join the fact_apj_sales and fact_apj_sale_items tables together, calculating the order total as the sum of the product costs. Only consider order states of 'COMPLETED' Order by the total order, descending.**
-- MAGIC
-- MAGIC
-- MAGIC Or you can use the query below. Make sure you update the catalog and schema name accordingly. 
-- MAGIC
-- MAGIC ```
-- MAGIC select 
-- MAGIC     a.ts,
-- MAGIC     a.sale_id,
-- MAGIC     a.order_source,
-- MAGIC     a.order_state,
-- MAGIC     a.unique_customer_id,
-- MAGIC     a.customer_skey,
-- MAGIC     a.store_id,
-- MAGIC     a.slocation_skey,
-- MAGIC     sum(b.product_cost) order_total
-- MAGIC from apjworkshop24.<my_schema>.fact_apj_sales a 
-- MAGIC     inner join apjworkshop24.<my_schema>.fact_apj_sale_items b
-- MAGIC     on a.sale_id = b.sale_id
-- MAGIC where 
-- MAGIC     a.order_state = 'COMPLETED'
-- MAGIC group by
-- MAGIC     all
-- MAGIC order by 
-- MAGIC     order_total desc;
-- MAGIC ```

-- COMMAND ----------

SELECT 
  s.*,
  SUM(si.product_cost) AS order_total
FROM 
  adrian_tompkins.adrian_tompkins.fact_apj_sales s
JOIN 
  adrian_tompkins.adrian_tompkins.fact_apj_sale_items si
ON 
  s.sale_id = si.sale_id
WHERE 
  s.order_state = 'COMPLETED'
GROUP BY 
  s.sale_id, s.ts, s.order_source, s.order_state, s.unique_customer_id, s.store_id, s.customer_skey, s.slocation_skey
ORDER BY 
  order_total DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fact_apj_sales = spark.table("adrian_tompkins.adrian_tompkins.fact_apj_sales")
-- MAGIC fact_apj_sale_items = spark.table("adrian_tompkins.adrian_tompkins.fact_apj_sale_items")
-- MAGIC
-- MAGIC joined_df = fact_apj_sales.join(fact_apj_sale_items, "sale_id") \
-- MAGIC     .filter(fact_apj_sales.order_state == "COMPLETED") \
-- MAGIC     .groupBy("sale_id") \
-- MAGIC     .agg({"product_cost": "sum"}) \
-- MAGIC     .withColumnRenamed("sum(product_cost)", "order_total") \
-- MAGIC     .orderBy("order_total", ascending=False)
-- MAGIC
-- MAGIC display(joined_df)

-- COMMAND ----------

CREATE OR REPLACE TABLE 
apjworkshop24.<my_schema>.top_orders_silver
AS
-- Copy select statement here
