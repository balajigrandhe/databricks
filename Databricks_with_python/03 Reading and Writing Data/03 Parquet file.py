# Databricks notebook source
df = spark.read.csv('/FileStore/tables/countries.csv', header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.parquet('/FileStore/tables/parquet')

# COMMAND ----------

display(spark.read.parquet('/FileStore/tables/parquet/part-00000-tid-6744234268475676450-f7a07f67-0592-4abb-b2d0-b4ded27fea4d-16-1-c000.snappy.parquet'))

# COMMAND ----------

