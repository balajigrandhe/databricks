# Databricks notebook source
print("Printing from Worker Notebook")

# COMMAND ----------

#dbutils.notebook.exit("Exit value of the note book")

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('input_widget', 'Enter Here', 'Provide Input')

# COMMAND ----------

dbutils.widgets.get('input_widget')

# COMMAND ----------

dbutils.widgets.text('input_widget1', 'Enter Here', 'Provide Input')

# COMMAND ----------

dbutils.widgets.get('input_widget1')