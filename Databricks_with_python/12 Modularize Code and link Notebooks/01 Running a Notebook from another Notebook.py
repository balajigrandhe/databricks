# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run('Worker Notebook', 60)

# COMMAND ----------

try:
    dbutils.notebook.run('Worker Notebook', 60)
except:
    print("Received error")

# COMMAND ----------

dbutils.notebook.run('/test', 60)

# COMMAND ----------

dbutils.notebook.run('Worker Notebook', 60, {'input_widget': 'Providing from master Notebook', 'input_widget2': 'Wow Master node'})

# COMMAND ----------

