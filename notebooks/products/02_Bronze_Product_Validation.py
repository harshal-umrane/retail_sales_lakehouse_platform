# Databricks notebook source
from pyspark.sql import *
import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

product_df = spark.read \
                .format("delta") \
                    .load("abfss://bronze@saretailsales.dfs.core.windows.net/products")

# COMMAND ----------

product_df.printSchema()

# COMMAND ----------

# COLUMN DATATYPE CORRECTION

product_df = product_df.withColumn("cost_price", col("cost_price").cast("integer")) \
                        .withColumn("createddate", col("createddate").cast("date")) \
                        .withColumn("price", col("price").cast("integer"))

product_df.printSchema()

# COMMAND ----------

display(product_df)

# COMMAND ----------

# Duplicate Product ID

duplicate_check = product_df.groupBy("product_id") \
                            .agg(count("product_id").alias("count")) \
                                .filter(col("count") > 1)

# Null Product ID
null_product_ids = product_df.select(col("product_id")).filter("product_id is null")

# Negative Price and Cost Price
negative_price = product_df.select(col("price")).filter(col("price") < 0)
negative_cost_price = product_df.select(col("cost_price")).filter(col("cost_price") < 0)
negative_stock = product_df.select(col("stock_quantity")).filter(col("stock_quantity") < 0)

#Future date check
future_date_check = product_df.select(col("launch_date")).filter(col("launch_date") > current_date())

display(future_date_check)

# COMMAND ----------

display(product_df)

# COMMAND ----------

