# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import *

# COMMAND ----------

orders_df = spark.read \
                .format("delta") \
                    .load("abfss://silver@saretailsales.dfs.core.windows.net/orders")

# COMMAND ----------

orders_df.count()

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

sales_daily_summary = (orders_df.groupBy("order_date")
                       .agg(
                           count("order_id").alias("total_orders"),
                           sum(when(col("order_status") == "Delivered", 1).otherwise(0)).alias("delivered_orders"),
                           sum(when(col("order_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_orders"),
                           sum(when(col("order_status") == "Returned", 1).otherwise(0)).alias("returned_orders"),
                           sum(col("quantity")).alias("total_quantity"),
                           round(sum(col("total_amount")), 2).alias("gross_sales"),
                           round(sum(col("discount_amount")), 2).alias("total_discount")
                           )
                       .withColumn("net_sales", round((col("gross_sales") - col("total_discount")), 2))
                       .withColumn("average_order_value", round(col("net_sales") / col("total_orders"), 2))
                       )

# display(sales_daily_summary)

sales_daily_summary.write \
                    .format("delta") \
                        .mode("overwrite") \
                            .save("abfss://gold@saretailsales.dfs.core.windows.net/orders/sales_daily_summary")

# COMMAND ----------

products = spark.read \
                .format("delta") \
                    .load("abfss://silver@saretailsales.dfs.core.windows.net/products")

product_sales_summary = (orders_df.groupBy("product_id")
                         .agg(
                             count("order_id").alias("total_orders"),
                             sum(col("quantity")).alias("total_quantity"),
                             round(sum(col("total_amount")), 2).alias("gross_sales"),
                             round(sum(col("discount_amount")), 2).alias("total_discount"),
                             round(sum(col("total_amount")) - sum(col("discount_amount")), 2).alias("net_sales"),
                             round(sum(col("total_amount")) / count("order_id"), 2).alias("average_order_value")
                             )
                        .join(products.select("product_id", "product_name", "category", "brand"), on="product_id", how="inner")
                        )

# display(product_sales_summary)

product_sales_summary.write \
                        .format("delta") \
                            .mode("overwrite") \
                                .save("abfss://gold@saretailsales.dfs.core.windows.net/orders/product_sales_summary")

# COMMAND ----------

customer_sales_summary = (orders_df.groupBy("customer_id")
                         .agg(
                             count("order_id").alias("total_orders"),
                             sum(col("quantity")).alias("total_quantity"),
                             round(sum(col("total_amount")), 2).alias("gross_sales"),
                             round(sum(col("total_amount")) / count("order_id"), 2).alias("average_order_value")
                             )
                         )

# display(customer_sales_summary)

customer_sales_summary.write \
                        .format("delta") \
                            .mode("overwrite") \
                                .save("abfss://gold@saretailsales.dfs.core.windows.net/orders/customer_sales_summary")

# COMMAND ----------

category_sales_summary = (orders_df.join(products.select("product_id", "category"), on="product_id", how="inner")
                          .groupBy("category")
                          .agg(
                             count("order_id").alias("total_orders"),
                             sum(col("quantity")).alias("total_quantity"),
                             round(sum(col("total_amount")), 2).alias("gross_sales"),
                             round(sum(col("discount_amount")), 2).alias("total_discount"),
                             round(sum(col("total_amount")) - sum(col("discount_amount")), 2).alias("net_sales"),
                             round(sum(col("total_amount")) / count("order_id"), 2).alias("average_order_value")
                          )
                         )

# display(category_sales_summary)

category_sales_summary.write \
                        .format("delta") \
                            .mode("overwrite") \
                                .save("abfss://gold@saretailsales.dfs.core.windows.net/orders/category_sales_summary")

# COMMAND ----------

