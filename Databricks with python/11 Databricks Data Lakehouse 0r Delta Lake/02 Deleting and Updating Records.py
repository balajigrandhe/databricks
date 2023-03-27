# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_lake_db.countries_managed_delta

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, '/user/hive/warehouse/delta_lake_db.db/countries_managed_delta')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("region_id = 20")

# Declare the predicate by using Spark SQL functions.
deltaTable.delete(col('birthDate') < '1960-01-01')

# COMMAND ----------

# MAGIC %sql
# MAGIC use delta_lake_db

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from countries_managed_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from countries_parquet where REGION_ID = 50

# COMMAND ----------

# MAGIC %sql
# MAGIC update countries_managed_delta
# MAGIC set COUNTRY_CODE = 'XXX'
# MAGIC where REGION_ID = 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from countries_managed_delta

# COMMAND ----------

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "region_id = 40",
  set = { "country_code": "'yyy'" }
)

# Declare the predicate by using Spark SQL functions.
deltaTable.update(
  condition = col('region_id') == 30,
  set = { 'country_code': lit('xyz') }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from countries_managed_delta

# COMMAND ----------

