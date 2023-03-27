# Databricks notebook source
customers = spark.read.parquet('/FileStore/tables/silver/customers')
order_items = spark.read.parquet('/FileStore/tables/silver/order_items')
orders = spark.read.parquet('/FileStore/tables/silver/orders')
products = spark.read.parquet('/FileStore/tables/silver/products')

# COMMAND ----------

orders.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

orders = orders.select('order_id',\
             to_date(orders['order_timestamp']).alias('DATE'),\
             'customer_id',\
             'store_name')

# COMMAND ----------

orders.display()

# COMMAND ----------

order_items.display()

# COMMAND ----------

order_details = orders.join(order_items, orders['order_id']==order_items['order_id'], 'left')\
                .select(orders['order_id'],\
                       orders['date'],\
                       orders['customer_id'],\
                       orders['store_name'],\
                       order_items['unit_price'],\
                       order_items['quantity'])

# COMMAND ----------

order_details = order_details.withColumn("TOTAL_ORDER_AMOUNT", round(order_details['unit_price']*order_details['quantity'],2))

# COMMAND ----------

order_details = order_details.drop('unit_price', 'quantity')
order_details.display()

# COMMAND ----------

order_details = order_details.groupBy('order_id', 'date', 'customer_id', 'store_name').sum('TOTAL_ORDER_AMOUNT').withColumnRenamed('sum(TOTAL_ORDER_AMOUNT)','TOTAL_ORDER_AMOUNT')

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = order_details.withColumn("TOTAL_ORDER_AMOUNT", round(order_details['total_order_amount'], 2))

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details.write.parquet('/FileStore/tables/gold/order_details', mode='overwrite')

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = order_details.withColumn('MONTH_DATE', date_format(order_details['date'], 'yyyy-MM'))

# COMMAND ----------

order_details.display()

# COMMAND ----------

monthly_sales = order_details.groupBy('month_date').sum('TOTAL_ORDER_AMOUNT').\
                    withColumn('TOTAL_SALES', round('sum(TOTAL_ORDER_AMOUNT)', 2)).\
                    sort(order_details['month_date'].desc()).\
                    select('month_date', 'total_sales')

# COMMAND ----------

monthly_sales.display()

# COMMAND ----------

monthly_sales.write.parquet('/FileStore/tables/gold/monthly_sales', mode='overwrite')

# COMMAND ----------

order_details.display()

# COMMAND ----------

store_monthly_sales = order_details.groupBy('MONTH_DATE', 'store_name').sum('TOTAL_ORDER_AMOUNT').\
withColumn('TOTAL_SALES', round('sum(TOTAL_ORDER_AMOUNT)', 2)).sort(order_details['month_date'].desc()).select('MONTH_DATE', 'STORE_NAME', 'TOTAL_SALES')

# COMMAND ----------

store_monthly_sales.display()

# COMMAND ----------

store_monthly_sales.write.parquet('/FileStore/tables/gold/store_monthly_sales', mode='overwrite')

# COMMAND ----------

