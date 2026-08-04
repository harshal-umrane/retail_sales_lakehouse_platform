# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
import datetime

PIPELINE_NAME = "03_Bronze_to_Silver"
SOURCE_SYSTEM = "Retail Sales"
BATCH_ID = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

# COMMAND ----------

product_df = spark.read \
                    .format("delta") \
                        .load("abfss://bronze@saretailsales.dfs.core.windows.net/products")

# COMMAND ----------

display(product_df.count())

# COMMAND ----------

product_df = product_df.withColumn("batch_id", lit(BATCH_ID)) \
                        .withColumn("source_system", lit(SOURCE_SYSTEM)) \
                        .withColumn("ingesttime", current_timestamp()) \
                        .withColumn("ingetstdate", current_date())

# COMMAND ----------

product_df.write \
            .mode("overwrite") \
                .format("delta") \
                    .save("abfss://silver@saretailsales.dfs.core.windows.net/products")

# COMMAND ----------

silver_df = spark.read \
                .format("delta") \
                .load("abfss://silver@saretailsales.dfs.core.windows.net/products")

display(silver_df)    

# COMMAND ----------

