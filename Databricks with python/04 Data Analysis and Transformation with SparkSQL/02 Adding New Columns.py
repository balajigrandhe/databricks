# Databricks notebook source
countries = spark.read.csv('/FileStore/tables/countries.csv',header=True)

# COMMAND ----------

from pyspark.sql.functions import current_date
countries.withColumn('TimeStamp', current_date()).display()

# COMMAND ----------

from pyspark.sql.functions import lit
countries.withColumn('Signature', lit('Balaji')).display()

# COMMAND ----------

countries.display()

# COMMAND ----------

