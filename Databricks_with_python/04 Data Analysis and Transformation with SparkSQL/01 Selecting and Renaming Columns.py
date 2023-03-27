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


# COMMAND ----------

countries = spark.read.csv(path=countries_path, header=True, schema=Countries_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.select('name','Capital', 'Population').display()

# COMMAND ----------

countries.select(countries['name'].alias('Country_name'), countries['capital'].alias('Country_city'), countries['Population']).display()

# COMMAND ----------

region = spark.read.csv('/FileStore/tables/country_regions.csv')

# COMMAND ----------

region.display()

# COMMAND ----------

region_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("NAME", StringType(), False)
])

# COMMAND ----------

region = spark.read.csv('/FileStore/tables/country_regions.csv', header=True, schema=region_schema)

# COMMAND ----------

region.display()

# COMMAND ----------

regions = region.select('ID','NAME').withColumnRenamed('NAME','CONTINENT')

# COMMAND ----------

regions.display()

# COMMAND ----------

