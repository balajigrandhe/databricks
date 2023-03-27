# Databricks notebook source
health_data = spark.read.format('delta').load('/mnt/health_updates/silver/health_data')

# COMMAND ----------

feeling_count_day = health_data.groupBy("date_provided", "feeling_today").count()

# COMMAND ----------

feeling_count_day.write.format('delta').mode('overwrite').option("path", "/mnt/health_updates/gold/feeling_count_day").saveAsTable("health_care.feeling_count_day")

# COMMAND ----------

symptoms_count_day = health_data.groupBy("date_provided", "general_symptoms").count()

# COMMAND ----------

symptoms_count_day.write.format('delta').mode('overwrite').option("path", "/mnt/health_updates/gold/symptoms_count_day").saveAsTable("health_care.symptoms_count_day")

# COMMAND ----------

healthcare_vist_day = health_data.groupBy("date_provided", "healthcare_visit").count()

# COMMAND ----------

healthcare_vist_day.write.format('delta').mode('overwrite').option("path", "/mnt/health_updates/gold/healthcare_vist_day").saveAsTable("health_care.healthcare_vist_day")

# COMMAND ----------

dbutils.notebook.exit("Processed from Silver to Gold")