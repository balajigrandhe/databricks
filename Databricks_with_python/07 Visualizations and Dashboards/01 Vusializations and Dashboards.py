# Databricks notebook source
monthly_sales = spark.read.parquet('/FileStore/tables/gold/monthly_sales')
order_details = spark.read.parquet('/FileStore/tables/gold/order_details')
store_montly_sales = spark.read.parquet('/FileStore/tables/gold/store_monthly_sales')

# COMMAND ----------

# MAGIC %md
# MAGIC This is new dashboard

# COMMAND ----------

display(order_details)

# COMMAND ----------

display(store_montly_sales)

# COMMAND ----------

