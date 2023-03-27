# Databricks notebook source
countries_path = '/FileStore/tables/countries.csv'

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField
Countries_schema = StructType([
    StructField("COUNTRY_ID", IntegerType(), False),
    StructField("NAME", StringType(), False),
    StructField("NATIONALITY", StringType(), False),
    StructField("COUNTRY_CODE", StringType(), False),
    StructField("ISO_ALPHA2", StringType(), False),
    StructField("CAPITAL", StringType(), False),
    StructField("POPULATION", DoubleType(), False),
    StructField("AREA_KM2", IntegerType(), False),
    StructField("REGION_ID", IntegerType(), True),
    StructField("SUB_REGION_ID", IntegerType(), True),
    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
    StructField("ORGANIZATION_REGION_ID", IntegerType(), True)
])

countries = spark.read.csv(path=countries_path, header=True, schema=Countries_schema)

# COMMAND ----------

countries.filter(countries['population']>100000000).display()

# COMMAND ----------

countries.filter("region_id == 10").display()

# COMMAND ----------

from pyspark.sql.functions import length
countries.filter((length(countries.NAME)>15) &(countries['region_id']!=10)).display()

# COMMAND ----------

