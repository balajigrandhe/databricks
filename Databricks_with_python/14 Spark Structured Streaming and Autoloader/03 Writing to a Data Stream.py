# Databricks notebook source
orders_path = '/mnt/streaming-demo/streaming_dataset/orders_streaming.csv'

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

orders_sdf = spark.readStream.csv(path=orders_path, schema=order_schema, header=True)

# COMMAND ----------

orders_sdf.display()

# COMMAND ----------

orders_sdf.writeStream.format('delta').\
option("checkpointLocation", "/mnt/streaming-demo/streaming_dataset/orders_stream_sink/_checkpointLocation").\
start("/mnt/streaming-demo/streaming_dataset/orders_stream_sink")

# COMMAND ----------

spark.read.format('delta').load("/mnt/streaming-demo/streaming_dataset/orders_stream_sink").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database streaming_db

# COMMAND ----------

orders_sdf.writeStream.format('delta').\
option("checkpointLocation", "/mnt/streaming-demo/streaming_dataset/streaming_db/managed/_checkpointLocation").\
toTable("streaming_db.orders_m")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from streaming_db.orders_m

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table streaming_db.orders_m

# COMMAND ----------

