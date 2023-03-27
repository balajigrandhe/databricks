# Databricks notebook source
spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countires_df = spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

type(countires_df)

# COMMAND ----------

display(countires_df)

# COMMAND ----------

countires_df = spark.read.options(header=True).csv("/FileStore/tables/countries.csv")

# COMMAND ----------

display(countires_df)

# COMMAND ----------

countries_df = spark.read.options(header=True, inferSchema=True).csv("/FileStore/tables/countries.csv")

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
countries_schema = StructType([
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
                    #StructField("ORGANIZATION_REGION_ID", IntegerType(), True)
                    ])

# COMMAND ----------

countries_df = spark.read.options(header=True).csv('/FileStore/tables/countries.csv', schema=countries_schema)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.csv('/FileStore/tables/countries.txt', header=True, sep='\t')

# COMMAND ----------

countries_df.show()

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_sl_json = spark.read.json('/FileStore/tables/countries_single_line.json')

# COMMAND ----------

display(countries_sl_json)

# COMMAND ----------

countries_ml_json = spark.read.json('/FileStore/tables/countries_multi_line.json', multiLine=True)

# COMMAND ----------

display(countries_ml_json)

# COMMAND ----------

