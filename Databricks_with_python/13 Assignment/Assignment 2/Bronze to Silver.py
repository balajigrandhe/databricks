# Databricks notebook source


# COMMAND ----------

application = dbutils.secrets.get(scope='databricks-secret-9101', key='application-id')
tenant = dbutils.secrets.get(scope='databricks-secret-9101', key='tenant-id')
secret = dbutils.secrets.get(scope='databricks-secret-9101', key='secret')

# COMMAND ----------

mount_point = '/mnt/health_updates'
container = 'health-updates'
account = 'datalake9101'

# COMMAND ----------

# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": application,
#           "fs.azure.account.oauth2.client.secret": secret,
#           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant}/oauth2/token"}

# # Optionally, you can add <directory-name> to the source URI of your mount point.
# dbutils.fs.mount(
#   source = f"abfss://{container}@{account}.dfs.core.windows.net/",
#   mount_point = mount_point,
#   extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #Processing the health_status_updates.csv to silver

# COMMAND ----------

health_status = spark.read.csv('/mnt/health_updates/bronze/health_status_updates.csv', header=True, inferSchema=True)

# COMMAND ----------

health_status.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
health_data = health_status.withColumn("UPDATED_TIMESTAMP", current_timestamp())

# COMMAND ----------

health_data.display()

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/health_updates/silver/health_data')

deltaTable.alias('tgt') \
  .merge(
    health_data.alias('src'),
    'src.status_update_id = tgt.status_update_id'
  ) \
  .whenMatchedUpdate(set =
    {
        "status_update_id":"src.status_update_id",
        "patient_id":"src.patient_id",
        "date_provided":"src.date_provided",
        "feeling_today":"src.feeling_today",
        "impact":"src.impact",
        "injection_site_symptoms":"src.injection_site_symptoms",
        "highest_temp":"src.highest_temp",
        "feverish_today":"src.feverish_today",
        "general_symptoms":"src.general_symptoms",
        "healthcare_visit": "src.healthcare_visit",
        "updated_timestamp": current_timestamp()
        
    }
  ) \
  .whenNotMatchedInsert(values =
    {
        "status_update_id":"src.status_update_id",
        "patient_id":"src.patient_id",
        "date_provided":"src.date_provided",
        "feeling_today":"src.feeling_today",
        "impact":"src.impact",
        "injection_site_symptoms":"src.injection_site_symptoms",
        "highest_temp":"src.highest_temp",
        "feverish_today":"src.feverish_today",
        "general_symptoms":"src.general_symptoms",
        "healthcare_visit": "src.healthcare_visit",
        "updated_timestamp": current_timestamp()
        
    }
  ) \
  .execute()

# COMMAND ----------

# health_data = spark.read.format('delta').load('/mnt/health_updates/silver/health_data', header=True, inferSchema=True)
# health_data.display()

# COMMAND ----------

dbutils.notebook.exit("Processed from Bronze to Silver")