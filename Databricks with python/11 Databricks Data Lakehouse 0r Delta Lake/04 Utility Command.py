# Databricks notebook source
# MAGIC %sql
# MAGIC describe history delta_lake_db.countries_1

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/user/hive/warehouse/delta_lake_db.db/countries_1')
deltaTable.history().display()

# COMMAND ----------

# Convert unpartitioned Parquet table at path '<path-to-table>'
deltaTable = DeltaTable.convertToDelta(spark, "parquet.`dbfs:/user/hive/warehouse/delta_lake_db.db/countries_parquet`")

# COMMAND ----------

type(deltaTable)

# COMMAND ----------

deltaTable.