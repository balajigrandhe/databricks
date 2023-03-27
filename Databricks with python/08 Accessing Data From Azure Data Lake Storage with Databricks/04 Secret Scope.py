# Databricks notebook source
application_id = "6e59ca00-f113-4116-8d93-017023a437ba"
tenant_id = "57926340-30d7-4338-b991-ee5542726027"
secret = "vcM8Q~53DIug7lcRSNzaDNipm6HWZv63V3Bs1bT1"

mount = "/mnt/bronze"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@datalake9101.dfs.core.windows.net/",
  mount_point = mount,
  extra_configs = configs)

# COMMAND ----------

#dbutils.fs.unmount(mount)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #Start from here

# COMMAND ----------

application_id = dbutils.secrets.get(scope="databricks-secret-9101", key="application-id")
tenant_id =  dbutils.secrets.get(scope="databricks-secret-9101", key="tenant-id")
secret = dbutils.secrets.get(scope="databricks-secret-9101", key="secret")

# COMMAND ----------

container_name = "bronze"
account_name = "datalake9101"
mount_point = "/mnt/bronze"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

