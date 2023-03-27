# Databricks notebook source
orders_path = '/FileStore/tables/bronze/orders.csv'
customers_path = '/FileStore/tables/bronze/customers.csv'
order_items_path = '/FileStore/tables/bronze/order_items.csv'
products_path = '/FileStore/tables/bronze/products.csv'
store_path = '/FileStore/tables/bronze/stores.csv'
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# COMMAND ----------

orders_schema = StructType([
    StructField("ORDER_ID", IntegerType(), False),
    StructField("ORDER_DATETIME", StringType(), False),
    StructField("CUSTOMER_ID", IntegerType(), False),
    StructField("ORDER_STATUS", StringType(), False),
    StructField("STORE_ID", IntegerType(), False)
])
orders = spark.read.csv(path=orders_path, header=True, schema=orders_schema)

# COMMAND ----------

orders.display()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

orders = orders.select('order_id',\
                      to_timestamp(orders['order_datetime'], 'dd-MMM-yy kk.mm.ss.SS').alias('ORDER_TIMESTAMP'),\
                      'customer_id',\
                       'order_status',\
                      'store_id')

# COMMAND ----------

orders.display()

# COMMAND ----------

orders = orders.filter( orders['order_status']=="COMPLETE").select('order_id', 'order_timestamp', 'customer_id', 'store_id')

# COMMAND ----------

stores_schema = StructType([
                    StructField("STORE_ID", IntegerType(), False),
                    StructField("STORE_NAME", StringType(), False),
                    StructField("WEB_ADDRESS", StringType(), False),
                    StructField("LATITUDE", DoubleType(), False),
                    StructField("LONGITUDE", DoubleType(), False)
                    ]
                    )

stores=spark.read.csv(path=store_path, header=True, schema=stores_schema)

# COMMAND ----------

stores.display()

# COMMAND ----------

orders = orders.join(stores, orders['store_id']==stores['store_id'], 'left').select('order_id', 'order_timestamp', 'customer_id', stores['store_name'])

# COMMAND ----------

orders.display()

# COMMAND ----------

orders.write.parquet('/FileStore/tables/silver/orders', mode='overwrite')

# COMMAND ----------

order_items_schema = StructType([
                    StructField("ORDER_ID", IntegerType(), False),
                    StructField("LINE_ITEM_ID", IntegerType(), False),
                    StructField("PRODUCT_ID", IntegerType(), False),
                    StructField("UNIT_PRICE", DoubleType(), False),
                    StructField("QUANTITY", IntegerType(), False)
                    ]
                    )

order_items=spark.read.csv(path=order_items_path, header=True, schema=order_items_schema)

# COMMAND ----------

order_items = order_items.select('order_id', 'product_id', 'unit_price', 'quantity')

# COMMAND ----------

order_items.display()

# COMMAND ----------

order_items.write.parquet('/FileStore/tables/silver/order_items', mode='overwrite')

# COMMAND ----------

product_schema = StructType([
    StructField("PRODUCT_ID", IntegerType(), False),
    StructField("PRODUCT_NAME", StringType(), False),
    StructField("UNIT_PRICE", DoubleType(), False)
])

products = spark.read.csv(path=products_path, header=True, schema=product_schema)

# COMMAND ----------

products.write.parquet('/FileStore/tables/silver/products', mode='overwrite')

# COMMAND ----------

customers_schema = StructType([
    StructField("CUSTOMER_ID", IntegerType(), False),
    StructField("FULL_NAME", StringType(), False),
    StructField("EMAIL_ADDRESS", StringType(), False)
])

customers = spark.read.csv(path=customers_path, header=True, schema=customers_schema)

# COMMAND ----------

customers.write.parquet('/FileStore/tables/silver/customers', mode='overwrite')

# COMMAND ----------

