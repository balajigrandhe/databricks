# Databricks notebook source
countries = spark.read.csv('/FileStore/tables/countries.csv', header=True)

# COMMAND ----------

countries.createTempView("countries_tv")

# COMMAND ----------

spark.sql("SELECT * FROM countries_tv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from countries_tv

# COMMAND ----------

countries.createOrReplaceTempView("countries_tv")

# COMMAND ----------

countries.createOrReplaceGlobalTempView("countries_gv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.countries_gv

# COMMAND ----------

