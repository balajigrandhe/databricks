# Databricks notebook source
# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC use countries;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries_txt

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries_txt

# COMMAND ----------

countries = spark.read.csv('/FileStore/tables/countries.csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC use default

# COMMAND ----------

countries.write.saveAsTable('countries.countries_mt')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries.countries_mt

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries.countries_mt

# COMMAND ----------

