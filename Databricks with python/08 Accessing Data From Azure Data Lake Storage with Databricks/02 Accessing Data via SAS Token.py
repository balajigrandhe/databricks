# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.datalake9101.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalake9101.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalake9101.dfs.core.windows.net", "sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-03-10T12:42:00Z&st=2023-03-10T04:42:00Z&spr=https&sig=hKbw%2B9k%2F3QSxbE0Zby96Sek0Za4uVh4kieA%2BsvCM7hs%3D")

# COMMAND ----------

countries = spark.read.csv("abfss://bronze@datalake9101.dfs.core.windows.net/countries.csv", header=True)
countries.display()

# COMMAND ----------

regions = spark.read.csv("abfss://bronze@datalake9101.dfs.core.windows.net/country_regions.csv", header=True)
regions.display()

# COMMAND ----------

