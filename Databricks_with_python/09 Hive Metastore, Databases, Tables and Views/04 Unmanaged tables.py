# Databricks notebook source
countries = spark.read.csv('/FileStore/tables/countries.csv', header=True)

# COMMAND ----------

countries.write.option('path', '/FileStore/external/countries').saveAsTable('countries.countries_ext')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries.countries_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries.countries_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC CREATE TABLE countries.countries_ext_sql
# MAGIC (country_id int,
# MAGIC name string,
# MAGIC nationality string,
# MAGIC country_code string,
# MAGIC iso_alpha_2 string,
# MAGIC capital string,
# MAGIC population int,
# MAGIC area_km2 int,
# MAGIC region_id int,
# MAGIC sub_region_id int,
# MAGIC intermediate_region_id int,
# MAGIC organization_region_id int)
# MAGIC USING CSV
# MAGIC LOCATION '/FileStore/tables/countries.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries.countries_ext_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries.countries_ext_sql

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.rm('/FileStore/external', recurse=True)

# COMMAND ----------

