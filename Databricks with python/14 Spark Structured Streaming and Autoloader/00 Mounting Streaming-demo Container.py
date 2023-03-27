# Databricks notebook source
application = dbutils.secrets.get(scope='databricks-secret-9101', key='application-id')
tenant = dbutils.secrets.get(scope='databricks-secret-9101', key='tenant-id')
secret = dbutils.secrets.get(scope='databricks-secret-9101', key='secret')

# COMMAND ----------

mount_point = '/mnt/streaming-demo'
container = 'streaming-demo'
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

