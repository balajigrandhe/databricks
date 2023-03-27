# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/countries.txt')

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/countries_multi_line.json')
dbutils.fs.rm('/FileStore/tables/countries_single_line.json')

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/without_header', recurse=True)

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/countries_out', recurse=True)
dbutils.fs.rm('/FileStore/tables/oputput', recurse=True)
dbutils.fs.rm('/FileStore/tables/parquet', recurse=True)

# COMMAND ----------

