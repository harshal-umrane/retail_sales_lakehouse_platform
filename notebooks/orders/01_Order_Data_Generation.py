# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import *
import random
from datetime import timedelta

# COMMAND ----------

customers = spark.read \
                .format("delta") \
                    .load("abfss://silver@saretailsales.dfs.core.windows.net/customers")

products = spark.read \
                .format("delta") \
                    .load("abfss://silver@saretailsales.dfs.core.windows.net/products") \
                        .filter(col("is_active") == True)

# COMMAND ----------

customer_keys = (
    customers
    .select("customer_id")
    .distinct()
    .withColumn(
        "customer_index",
        row_number().over(
            Window.orderBy("customer_id")
        ) - 1
    )
)

product_keys = (
    products
    .select("product_id")
    .distinct()
    .withColumn(
        "product_index",
        row_number().over(
            Window.orderBy("product_id")
        ) - 1
    )
)

customer_count = customer_keys.count()
product_count = product_keys.count()

# COMMAND ----------

orders = (
    spark.range(1, 200001)
    .withColumnRenamed("id", "order_id")
)

orders = orders.withColumn("customer_index",pmod(xxhash64("order_id", lit("customer")),lit(customer_count)))

orders = (orders.join(customer_keys, on="customer_index", how="inner").drop("customer_index"))

orders = orders.withColumn("product_index",pmod(xxhash64("order_id", lit("product")),lit(product_count)))

orders = (orders.join(broadcast(product_keys),on="product_index",how="inner").drop("product_index"))

orders = (orders.join(customers.select("customer_id","registration_date"), on="customer_id", how="inner"))

orders = (orders.join(broadcast(products.select("product_id","price","launch_date")),on="product_id",how="inner"))

orders = orders.withColumnRenamed("price","unit_price")

orders = orders.withColumn("min_order_date",greatest(col("registration_date"),col("launch_date")))

orders = orders.withColumn( "order_date",
                           expr("""date_add(min_order_date,
                                cast(floor(rand(100) * datediff(current_date(),min_order_date)) as int))"""))

orders = orders.withColumn("delivery_date",date_add(col("order_date"),(floor(rand(200) * 7) + 1).cast("int")))

orders = orders.withColumn("quantity",(floor(rand(42) * 5) + 1).cast("int"))

orders = orders.withColumn("discount_percentage",expr("array(0,5,10,15,20)[cast(floor(rand(7)*5) as int)]"))

orders = orders.withColumn("discount_amount",
                           round(col("unit_price") * col("quantity") * col("discount_percentage") / 100,2))

orders = orders.withColumn("total_amount",round((col("unit_price") * col("quantity")) - col("discount_amount"), 2))

orders = orders.withColumn("order_status",expr( """ CASE 
                                                    WHEN rand(300) < 0.85 THEN 'Delivered'
                                                    WHEN rand(301) < 0.93 THEN 'Cancelled'
                                                    ELSE 'Returned'
                                                    END """))

orders = orders.withColumn("order_channel",expr( """CASE 
                                                    WHEN rand(400) < 0.50 THEN 'Website' 
                                                    WHEN rand(401) < 0.80 THEN 'Android App' 
                                                    ELSE 'IOS App' 
                                                    END """))

orders = orders.withColumn("payment_method",expr("""CASE
                                                    WHEN rand(1) < 0.35 THEN 'UPI'
                                                    WHEN rand(2) < 0.60 THEN 'Credit Card'
                                                    WHEN rand(3) < 0.80 THEN 'Debit Card'
                                                    WHEN rand(4) < 0.90 THEN 'Net Banking'
                                                    ELSE 'Cash'
                                                    END """))

orders = orders.select(
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_percentage",
    "discount_amount",
    "total_amount",
    "payment_method",
    "order_status",
    "order_date",
    "delivery_date",
    "order_channel"
)

orders_df = orders

# COMMAND ----------

orders_df.write \
            .format("delta") \
                .mode("overwrite") \
                    .save("abfss://bronze@saretailsales.dfs.core.windows.net/orders")