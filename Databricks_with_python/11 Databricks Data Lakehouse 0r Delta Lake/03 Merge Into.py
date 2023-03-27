# Databricks notebook source
countries = spark.read.csv('/mnt/bronze/countries.csv', header=True, inferSchema=True)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries_1 = countries.filter("region_id in (10,20,30)")

# COMMAND ----------

countries_1.display()

# COMMAND ----------

countries_2 = countries.filter("region_id in (20,30,40,50)")

# COMMAND ----------

countries_2.display()

# COMMAND ----------

countries_1.write.format('delta').saveAsTable('delta_lake_db.countries_1') 

# COMMAND ----------

countries_2.write.format('delta').saveAsTable('delta_lake_db.countries_2') 

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta_lake_db.countries_1
# MAGIC set name = upper(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_lake_db.countries_1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO delta_lake_db.countries_1 tgt
# MAGIC USING delta_lake_db.countries_2 src
# MAGIC on tgt.country_id = src.country_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.name = src.name
# MAGIC WHEN NOT MATCHED then
# MAGIC   INSERT 
# MAGIC   (
# MAGIC     tgt.country_id,
# MAGIC     tgt.name,
# MAGIC     tgt.nationality,
# MAGIC     tgt.country_code,
# MAGIC     tgt.iso_alpha2,
# MAGIC     tgt.capital,
# MAGIC     tgt.population,
# MAGIC     tgt.area_km2,
# MAGIC     tgt.region_id,
# MAGIC     tgt.sub_region_id,
# MAGIC     tgt.intermediate_region_id,
# MAGIC     tgt.organization_region_id
# MAGIC   )
# MAGIC   values
# MAGIC   (
# MAGIC     src.country_id,
# MAGIC     src.name,
# MAGIC     src.nationality,
# MAGIC     src.country_code,
# MAGIC     src.iso_alpha2,
# MAGIC     src.capital,
# MAGIC     src.population,
# MAGIC     src.area_km2,
# MAGIC     src.region_id,
# MAGIC     src.sub_region_id,
# MAGIC     src.intermediate_region_id,
# MAGIC     src.organization_region_id
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table delta_lake_db.countries_1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_lake_db.countries_1

# COMMAND ----------

