# Databricks notebook source
orders_full = spark.read.csv('/mnt/streaming-demo/full_dataset/orders_full.csv', header=True, inferSchema=True)

# COMMAND ----------

order_1 = orders_full.filter(orders_full['order_id']==1)
order_1.write.options(header=True).csv('/mnt/streaming-demo/streaming_dataset/orders_streaming.csv')

# COMMAND ----------

order_2 = orders_full.filter(orders_full['order_id']==2)
order_2.write.options(header=True).mode('append').csv('/mnt/streaming-demo/streaming_dataset/orders_streaming.csv')

# COMMAND ----------

order_3 = orders_full.filter(orders_full['order_id']==3)
order_3.write.mode('append').csv('/mnt/streaming-demo/streaming_dataset/orders_streaming.csv', header=True)

# COMMAND ----------

order_4_5 = orders_full.filter((orders_full['order_id']==4) | (orders_full['order_id']==5))
order_4_5.write.mode('append').csv('/mnt/streaming-demo/streaming_dataset/orders_streaming.csv', header=True)

# COMMAND ----------

order_6_7 = orders_full.filter(orders_full['order_id']==6 | orders_full['order_id']==7)
order_6_7.write.mode('append').csv('/mnt/streaming-demo/streaming_dataset/orders_streaming.csv', header=True)