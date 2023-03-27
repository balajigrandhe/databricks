# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

application_id = "6e59ca00-f113-4116-8d93-017023a437ba"

tenant_id = "57926340-30d7-4338-b991-ee5542726027"

secret = "vcM8Q~53DIug7lcRSNzaDNipm6HWZv63V3Bs1bT1"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "6e59ca00-f113-4116-8d93-017023a437ba",
          "fs.azure.account.oauth2.client.secret": "vcM8Q~53DIug7lcRSNzaDNipm6HWZv63V3Bs1bT1",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/57926340-30d7-4338-b991-ee5542726027/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@datalake9101.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

countries = spark.read.csv("/mnt/bronze/countries.csv", header=True)
countries.display()

# COMMAND ----------

dbutils.fs.unmount('/mnt/bronze')

# COMMAND ----------

