# Databricks notebook source
countries = spark.read.csv('/FileStore/tables/countries.csv', header=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database countries

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC use countries

# COMMAND ----------

countries.write.saveAsTable('countries_mt')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries_mt

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view region_with_10
# MAGIC as select * from countries_mt
# MAGIC where region_id = 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from region_with_10

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database countries cascade

# COMMAND ----------

