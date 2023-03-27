# Databricks notebook source
application = dbutils.secrets.get(scope='databricks-secret-9101', key='application-id')
tenant = dbutils.secrets.get(scope='databricks-secret-9101', key='tenant-id')
secret = dbutils.secrets.get(scope='databricks-secret-9101', key='secret')

# COMMAND ----------

mount_point = '/mnt/health_updates'
container = 'health-updates'
account = 'datalake9101'

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container}@{account}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #Processing the health_status_updates.csv to silver

# COMMAND ----------

health_status = spark.read.csv('/mnt/health_updates/bronze/health_status_updates.csv', header=True, inferSchema=True)

# COMMAND ----------

health_status.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
health_status = health_status.withColumn("UPDATED_TIMESTAMP", current_timestamp())

# COMMAND ----------

health_status.display()

# COMMAND ----------

health_status.write.format('delta').save('/mnt/health_updates/silver/health_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC create database health_care

# COMMAND ----------

health_data = spark.read.format('delta').load('/mnt/health_updates/silver/health_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table health_care.health_data

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists health_care.health_data
# MAGIC (
# MAGIC STATUS_UPDATE_ID int,
# MAGIC PATIENT_ID int,
# MAGIC DATE_PROVIDED string,
# MAGIC FEELING_TODAY string,
# MAGIC IMPACT string,
# MAGIC INJECTION_SITE_SYMPTOMS string,
# MAGIC HIGHEST_TEMP double,
# MAGIC FEVERISH_TODAY string,
# MAGIC GENERAL_SYMPTOMS string,
# MAGIC HEALTHCARE_VISIT string,
# MAGIC UPDATED_TIMESTAMP timestamp
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION '/mnt/health_updates/silver/health_data'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended health_care.health_data

# COMMAND ----------

health_data.write.format('delta').option('path', '/mnt/health_updates/silver/health_data').saveAsTable('health_care.health_data')

# COMMAND ----------

