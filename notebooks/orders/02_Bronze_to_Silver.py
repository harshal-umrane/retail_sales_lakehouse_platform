# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import *
from datetime import datetime

PIPELINE_NAME = "03_Bronze_to_Silver"
SOURCE_SYSTEM = "Retail Sales"
BATCH_ID = datetime.now().strftime("%Y%m%d%H%M%S")

# COMMAND ----------

orders_df = spark.read \
                .format("delta") \
                    .load("abfss://bronze@saretailsales.dfs.core.windows.net/orders")

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

orders_df = (orders_df
    .withColumn("batch_id", lit(BATCH_ID))
    .withColumn("source_system", lit(SOURCE_SYSTEM))
    .withColumn("pipeline_name", lit(PIPELINE_NAME))
    .withColumn("ingesttime", current_timestamp())
    .withColumn("ingestdate", current_date())
)

# COMMAND ----------

orders_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("abfss://silver@saretailsales.dfs.core.windows.net/orders")

# COMMAND ----------

