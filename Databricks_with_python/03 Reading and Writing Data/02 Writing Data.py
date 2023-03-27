# Databricks notebook source
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

countries_df = spark.read.csv('/FileStore/tables/countries.csv', header=True, schema=countries_schema)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.write.csv('/FileStore/tables/countries_out', header=True)

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/countries_out', header=True)
display(df)

# COMMAND ----------

countries_df.write.options(header=True).mode('overwrite').csv('/FileStore/tables/countries_out')

# COMMAND ----------

countries_df.write.options(header=True).partitionBy('REGION_ID').mode('overwrite').csv('/FileStore/tables/oputput/countries_out')

# COMMAND ----------

countries_df.write.options(header=True).partitionBy('REGION_ID','SUB_REGION_ID').mode('overwrite').csv('/FileStore/tables/countries_out')

# COMMAND ----------

countries_df.write.csv('/FileStore/tables/without_header')

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/without_header/part-00000-tid-6630384442196337979-51d60fce-8060-4032-b9a5-8edb5a5be3ed-9-1-c000.csv', header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

