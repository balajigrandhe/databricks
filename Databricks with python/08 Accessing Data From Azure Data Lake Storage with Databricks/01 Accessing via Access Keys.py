# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.datalake9101.dfs.core.windows.net",
    "q9jnN6Whhortm0qdpsN9VC3+s9MN5w6Kvgy9x/kTzKcpVVDDZWAV5BZdE07WdWL2+KPjmH3haAx0+AStyQ6xMA==")

# COMMAND ----------

countries = spark.read.csv("abfss://bronze@datalake9101.dfs.core.windows.net/countries.csv", header=True)
countries.display()

# COMMAND ----------

countries_region = spark.read.csv("abfss://bronze@datalake9101.dfs.core.windows.net/country_regions.csv", header=True)
countries_region.display()

# COMMAND ----------

