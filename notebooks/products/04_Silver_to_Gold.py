# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

products_silver_df = spark.read \
                            .format("delta") \
                                .load("abfss://silver@saretailsales.dfs.core.windows.net/products")

# COMMAND ----------

display(products_silver_df.count())

# COMMAND ----------

products_silver_df.printSchema()


# COMMAND ----------

# Product Category summary

product_category_summary = products_silver_df \
                                .groupBy("category") \
                                    .agg(count("product_id").alias("total_products"),
                                         round(avg("price")).alias("avg_price"),
                                         round(avg("cost_price")).alias("avg_cost_price"))

# display(product_category_summary)

product_category_summary.write \
                        .mode("overwrite") \
                            .format("delta") \
                                .save("abfss://gold@saretailsales.dfs.core.windows.net/products/product_category_summary")

# COMMAND ----------

# Brand Summary

brand_summary = products_silver_df \
                    .groupBy("brand") \
                        .agg(count("product_id").alias("total_products"))

# display(brand_summary)

brand_summary.write \
            .mode("overwrite") \
                .format("delta") \
                    .save("abfss://gold@saretailsales.dfs.core.windows.net/products/brand_summary")


# COMMAND ----------

# Inventory Summary

inventory_summary = products_silver_df \
                    .groupBy("category") \
                        .agg(count("stock_quantity").alias("total_stock"))

# display(inventory_summary)

inventory_summary.write \
            .mode("overwrite") \
                .format("delta") \
                    .save("abfss://gold@saretailsales.dfs.core.windows.net/products/inventory_summary")


# COMMAND ----------

max_min = products_silver_df \
            .groupBy("category") \
                .agg(
                    min("price").alias("min_price"),
                    max("price").alias("max_price")
                    )

display(max_min)

# COMMAND ----------

#Category wise price band summary

price_band_summary = (products_silver_df.withColumn("price_band",
        when(
            (col("category") == "Home") & (col("price") < 5000),
            "Budget"
        ).when(
            (col("category") == "Home") & (col("price") < 20000),
            "Mid Range"
        ).when(
            col("category") == "Home",
            "Premium"
        ).when(
            (col("category") == "Beauty") & (col("price") < 1000),
            "Budget"
        ).when(
            (col("category") == "Beauty") & (col("price") < 2000),
            "Mid Range"
        ).when(
            col("category") == "Beauty",
            "Premium"
        ).when(
            (col("category") == "Grocery") & (col("price") < 500),
            "Budget"
        ).when(
            (col("category") == "Grocery") & (col("price") < 1000),
            "Mid Range"
        ).when(
            col("category") == "Grocery",
            "Premium"
        ).when(
            (col("category") == "Electronics") & (col("price") < 15000),
            "Budget"
        ).when(
            (col("category") == "Electronics") & (col("price") < 50000),
            "Mid Range"
        ).when(
            col("category") == "Electronics",
            "Premium"
        ).when(
            (col("category") == "Fashion") & (col("price") < 2000),
            "Budget"
        ).when(
            (col("category") == "Fashion") & (col("price") < 5000),
            "Mid Range"
        ).when(
            col("category") == "Fashion",
            "Premium"
        )
    )
    .groupBy("category", "price_band") \
        .agg(count("product_id").alias("product_count")) \
            .select("category", "price_band", "product_count") \
                .orderBy("category", "price_band", ascending=True)
)

# display(price_band_summary)

price_band_summary.write \
            .mode("overwrite") \
                .format("delta") \
                    .save("abfss://gold@saretailsales.dfs.core.windows.net/products/price_band_summary")
                          


# COMMAND ----------

