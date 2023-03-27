# Databricks notebook source
ORDER_ID:integer
ORDER_DATETIME:string
CUSTOMER_ID:integer
ORDER_STATUS:string
STORE_ID:integer

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType
order_schema = StructType([
    StructField("ORDER_ID", IntegerType(), False),
    StructField("ORDER_DATETIME", StringType(), False),
    StructField("CUSTOMER_ID", IntegerType(), False),
    StructField("ORDER_STATUS", StringType(), False),
    StructField("STORE_ID", IntegerType(), False),
])

# COMMAND ----------

orders_sdf = spark.readStream.csv('/mnt/streaming-demo/streaming_dataset/orders_streaming.csv', order_schema, header=True)

# COMMAND ----------

orders_sdf.isStreaming

# COMMAND ----------

orders_sdf.display()

# COMMAND ----------

