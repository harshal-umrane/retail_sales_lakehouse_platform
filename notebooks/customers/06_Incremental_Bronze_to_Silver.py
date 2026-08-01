# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import *
from delta import DeltaTable

PIPELINE_NAME = "06_Incremental_Bronze_to_Silver"
SOURCE_SYSTEM = "Retail Sales"
BATCH_ID = datetime.now().strftime("%Y%m%d%H%M%S")

# COMMAND ----------

# Loading Data from Silver and Bronze Container
silver_df = spark.read \
            .format("delta") \
                .load("abfss://silver@saretailsales.dfs.core.windows.net/customers")

incremental_load = spark.read \
            .format("delta") \
                .load("abfss://bronze@saretailsales.dfs.core.windows.net/customer_incremental")

# COMMAND ----------

# Standardize customer name with capital initial letter
incremental_load = incremental_load.withColumn("first_name", initcap(trim(col("first_name"))))

incremental_load = incremental_load.withColumn("last_name", initcap(trim(col("last_name"))))

# Standardize gender and email
incremental_load = incremental_load.withColumn("gender", when(lower(trim(col("gender"))).isin("m", "male"), "Male")
                                                .when(lower(trim(col("gender"))).isin("f", "female"), "Female")
                                                .otherwise (col("gender")))

incremental_load = incremental_load.withColumn("email", lower(trim(col("email"))))

# COMMAND ----------

# Filter out duplicate customer emails and customer ids
incremental_load.groupBy("customer_id") \
            .count() \
            .filter("count > 1") \
            .display()

incremental_load.groupBy("email") \
            .count() \
            .filter("count > 1") \
            .display()

# # Drop duplicate customerid and email
incremental_load = incremental_load.dropDuplicates(["customer_id"])
incremental_load = incremental_load.dropDuplicates(["email"])

# COMMAND ----------

# Adding Metadata to the dataframe
incremental_load = incremental_load.withColumn("batch_id", lit(BATCH_ID)) \
    .withColumn("source_system", lit(SOURCE_SYSTEM)) \
    .withColumn("pipeline_name", lit(PIPELINE_NAME)) \
    .withColumn("ingesttime", when(~col("customer_id").isin([row.customer_id for row in silver_df.select("customer_id").collect()]), current_timestamp())) \
    .withColumn("ingestdate", when(~col("customer_id").isin([row.customer_id for row in silver_df.select("customer_id").collect()]), current_date())) \
    .withColumn("modified_time", when(col("customer_id").isin([row.customer_id for row in silver_df.select("customer_id").collect()]), current_timestamp())) \
    .withColumn("modified_date", when(col("customer_id").isin([row.customer_id for row in silver_df.select("customer_id").collect()]), current_date()))

# COMMAND ----------

silver_table = DeltaTable.forPath(spark, 'abfss://silver@saretailsales.dfs.core.windows.net/customers')

# COMMAND ----------

silver_table.alias("target").merge(incremental_load.alias("source"),"target.customer_id = source.customer_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()


# COMMAND ----------

silver_df = spark.read \
            .format("delta") \
                .load("abfss://silver@saretailsales.dfs.core.windows.net/customers")
                

# COMMAND ----------

print(silver_df.count())

# COMMAND ----------

silver_df.filter(silver_df.customer_id > 1000).orderBy("customer_id", ascending=True).display()

# COMMAND ----------

