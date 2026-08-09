# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import *

# COMMAND ----------

customers_df = spark.read \
                .format("delta") \
                    .load("abfss://silver@saretailsales.dfs.core.windows.net/customers")

products_df = spark.read \
                .format("delta") \
                    .load("abfss://silver@saretailsales.dfs.core.windows.net/products")

orders_df = spark.read \
                .format("delta") \
                    .load("abfss://bronze@saretailsales.dfs.core.windows.net/orders")

# COMMAND ----------

orders_df.count()

# COMMAND ----------

print("Total Orders: ", orders_df.count())

print("Distinct Orders: ", orders_df.select("order_id").distinct().count())

print("Null Customers ID: ", orders_df.filter(col("customer_id").isNull()).count())

print("Null Product ID: ", orders_df.filter(col("product_id").isNull()).count())

print("Invalid Quantity: ", orders_df.filter(col("quantity") <= 0).count())

print("Invalid Total Amount: ", orders_df.filter(col("total_amount") <= 0).count())

print("Invalid Delivery Date: ", orders_df.filter(col("delivery_date") < col("order_date")).count())

print("Invalid Order Date: ", orders_df.filter((col("order_date") < col("registration_date")) | (col("order_date") < col("launch_date"))).count())

# COMMAND ----------

missing_customers = (orders_df.select("customer_id").distinct().join(customers_df.select("customer_id").distinct(),on="customer_id",how="left_anti"))

missing_products = (orders_df.select("product_id").distinct().join(products_df.select("product_id").distinct(),on="product_id",how="left_anti"))

print("Missing Product IDs:", missing_products.count())

print("Missing Customer IDs:", missing_customers.count())

# COMMAND ----------

