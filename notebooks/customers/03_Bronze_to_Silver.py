# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import *

PIPELINE_NAME = "03_Bronze_to_Silver"
SOURCE_SYSTEM = "Retail Sales"
BATCH_ID = datetime.now().strftime("%Y%m%d%H%M%S")

# COMMAND ----------

customer_df = spark.read \
    .format("delta") \
    .load("abfss://bronze@saretailsales.dfs.core.windows.net/customers/")

# COMMAND ----------

# Standardize customer name with capital initial letter
customer_df = customer_df.withColumn("first_name", initcap(trim(col("first_name"))))

customer_df = customer_df.withColumn("last_name", initcap(trim(col("last_name"))))

# Standardize gender and email
customer_df = customer_df.withColumn("gender", when(lower(trim(col("gender"))).isin("m", "male"), "Male")
                                                .when(lower(trim(col("gender"))).isin("f", "female"), "Female")
                                                .otherwise (col("gender")))

customer_df = customer_df.withColumn("email", lower(trim(col("email"))))

# Typecasting columns with correct data types
customer_df = customer_df.withColumn("phone", col("phone").cast("long"))

customer_df = customer_df.withColumn("registration_date", to_date(col("registration_date")))

customer_df = customer_df.withColumn("pincode", col("pincode").cast("long")) 

display(customer_df)

# COMMAND ----------

# Filter out duplicate customer emails and customer ids
customer_df.groupBy("customer_id") \
            .count() \
            .filter("count > 1") \
            .display()

customer_df.groupBy("email") \
            .count() \
            .filter("count > 1") \
            .display()

# Drop duplicate emails
customer_df = customer_df.dropDuplicates(["email"])

display(customer_df)


# COMMAND ----------

display(customer_df)

# COMMAND ----------

# Adding Metadata to the dataframe
customer_df = customer_df.withColumn("batch_id", lit(BATCH_ID)) \
                        .withColumn("source_system", lit(SOURCE_SYSTEM)) \
                        .withColumn("pipeline_name", lit(PIPELINE_NAME)) \
                        .withColumn("ingesttime", current_timestamp()) \
                        .withColumn("ingestdate", current_date())

display(customer_df)

# COMMAND ----------

customer_df.write \
    .format("delta") \
        .mode("overwrite") \
        .save("abfss://silver@saretailsales.dfs.core.windows.net/customers/")

# COMMAND ----------

silver_df = spark.read \
    .format("delta") \
    .load("abfss://silver@saretailsales.dfs.core.windows.net/customers/")

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df.count()

# COMMAND ----------

