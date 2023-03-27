# Databricks notebook source
application = dbutils.secrets.get(scope='databricks-secret-9101', key='application-id')
tenant = dbutils.secrets.get(scope='databricks-secret-9101', key='tenant-id')
secret = dbutils.secrets.get(scope='databricks-secret-9101', key='secret')

container_name = 'delta-lake-demo'
mount_point = '/mnt/delta-lake-demo'
account_name = 'datalake9101'

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

countries = spark.read.csv('dbfs:/mnt/bronze/countries.csv', header=True, inferSchema=True)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.write.format('delta').save('/mnt/delta-lake-demo/countries_dlt')

# COMMAND ----------

countries.write.format('parquet').save('/mnt/delta-lake-demo/countries_parquet')

# COMMAND ----------

spark.read.format('delta').load('/mnt/delta-lake-demo/countries_dlt').display()

# COMMAND ----------

countries.write.format('delta').partitionBy("region_id").save('/mnt/delta-lake-demo/countries_delta_part')

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists delta_lake_db;

# COMMAND ----------

countries = spark.read.format('delta').load('/mnt/delta-lake-demo/countries_dlt')

# COMMAND ----------

countries.write.saveAsTable('delta_lake_db.countries_managed_delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended delta_lake_db.countries_managed_delta

# COMMAND ----------

countries.write.option('path', '/mnt/delta-lake-demo/countries_dlt').mode('overwrite').saveAsTable('delta_lake_db.countries_external_delta')

# COMMAND ----------

countries = spark.read.parquet('/mnt/delta-lake-demo/countries_parquet')

# COMMAND ----------

countries.write.format('parquet').saveAsTable('delta_lake_db.countries_parquet')

# COMMAND ----------

